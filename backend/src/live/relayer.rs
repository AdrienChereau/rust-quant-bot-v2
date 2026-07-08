//! MERGE + REDEEM on-chain via le relayer officiel Polymarket (gasless) —
//! implémenté depuis la doc officielle (docs.polymarket.com) :
//!   trading/gasless.md · trading/deposit-wallets.md · trading/ctf/{merge,redeem}.md
//!   resources/contracts.md · api-reference/relayer/*
//!
//! Chemin deposit wallet (notre sig_type 3) :
//!   1. GET  /nonce?address=<EOA>&type=WALLET            → nonce du wallet
//!   2. Batch EIP-712 signé par l'EOA (65 octets), domaine
//!      { name:"DepositWallet", version:"1", chainId:137, verifyingContract:<wallet> }
//!   3. POST /submit { type:"WALLET", from, to:<factory>, nonce, signature,
//!      depositWalletParams:{ depositWallet, deadline, calls:[{target,value,data}] } }
//!   4. GET  /transaction?id={id} jusqu'à STATE_CONFIRMED (ou échec)
//!
//! Calls encodés (marchés binaires simples, partition [1,2]) :
//!   merge  → CtfCollateralAdapter.mergePositions(pUSD, 0x0, conditionId, [1,2], amount)
//!   redeem → CtfCollateralAdapter.redeemPositions(pUSD, 0x0, conditionId, [1,2])
//!            (brûle TOUT le solde de la condition — pas de paramètre montant)
//!   + approbation unique : CTF.setApprovalForAll(adapter, true) (préfixée au 1er batch)
//!
//! Auth : en-têtes RELAYER_API_KEY / RELAYER_API_KEY_ADDRESS (self-service
//! polymarket.com → Settings → API Keys).

use std::str::FromStr as _;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync as _;
use alloy::sol;
use alloy::sol_types::{eip712_domain, SolCall, SolStruct};

use super::auth::{LiveCredentials, HTTP};

const RELAYER_BASE: &str = "https://relayer-v2.polymarket.com";
const DEPOSIT_WALLET_FACTORY: &str = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07";
const CTF: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const CTF_COLLATERAL_ADAPTER: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";
const PUSD: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
const BASE_UNITS: f64 = 1_000_000.0;

sol! {
    struct Call {
        address target;
        uint256 value;
        bytes data;
    }
    struct Batch {
        address wallet;
        uint256 nonce;
        uint256 deadline;
        Call[] calls;
    }
    function mergePositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] partition, uint256 amount);
    function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets);
    function setApprovalForAll(address operator, bool approved);
}

pub struct RelayerCtx {
    creds: LiveCredentials,
    api_key: String,
    api_key_address: String,
    #[allow(dead_code)]
    approval_done: bool, // (historique — l'approbation est désormais dans chaque batch)
}

#[derive(Debug)]
pub enum TxOutcome {
    Confirmed,
    Failed(String),
}

impl RelayerCtx {
    /// None si les clés relayer ne sont pas dans l'env → merge/redeem désactivés
    /// (le bot vit très bien sans, il tient juste ses positions à la résolution).
    pub fn from_env(creds: &LiveCredentials) -> Option<Self> {
        let api_key = std::env::var("RELAYER_API_KEY").ok()?.trim().to_string();
        let api_key_address = std::env::var("RELAYER_API_KEY_ADDRESS")
            .ok()
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| creds.signer_address.clone());
        if api_key.is_empty() {
            return None;
        }
        Some(Self {
            creds: creds.clone(),
            api_key,
            api_key_address,
            approval_done: false,
        })
    }

    /// MERGE de `pairs` paires (Up+Down → pUSD dans le wallet). Bloquant jusqu'à
    /// soumission (la confirmation est suivie par la task retournée).
    pub async fn merge(
        &mut self,
        condition_id: &str,
        pairs: f64,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<TxOutcome>> {
        let amount = U256::from((pairs * BASE_UNITS).round() as u128);
        let data = mergePositionsCall {
            collateralToken: addr(PUSD)?,
            parentCollectionId: B256::ZERO,
            conditionId: b256(condition_id)?,
            partition: vec![U256::from(1u8), U256::from(2u8)],
            amount,
        }
        .abi_encode();
        self.submit_batch("merge", condition_id, data).await
    }

    /// REDEEM après résolution : brûle TOUT le solde de la condition, rend le pUSD.
    pub async fn redeem(
        &mut self,
        condition_id: &str,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<TxOutcome>> {
        let data = redeemPositionsCall {
            collateralToken: addr(PUSD)?,
            parentCollectionId: B256::ZERO,
            conditionId: b256(condition_id)?,
            indexSets: vec![U256::from(1u8), U256::from(2u8)],
        }
        .abi_encode();
        self.submit_batch("redeem", condition_id, data).await
    }

    async fn submit_batch(
        &mut self,
        op: &str,
        condition_id: &str,
        adapter_calldata: Vec<u8>,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<TxOutcome>> {
        let wallet = addr(&self.creds.funder)?;
        let owner = &self.creds.signer_address;

        // 1. nonce WALLET courant
        let nonce_txt = self
            .relayer_get(&format!("/nonce?address={owner}&type=WALLET"))
            .await?;
        let nonce_v: serde_json::Value = serde_json::from_str(&nonce_txt)
            .map_err(|e| anyhow::anyhow!("nonce JSON '{nonce_txt}': {e}"))?;
        let nonce_field = nonce_v.get("nonce");
        let nonce: u64 = match nonce_field {
            Some(serde_json::Value::String(s)) => s.parse().ok(),
            Some(serde_json::Value::Number(n)) => n.as_u64(),
            _ => None,
        }
        .ok_or_else(|| anyhow::anyhow!("nonce absent: {nonce_txt}"))?;

        // 2. batch : l'approbation de l'adaptateur est incluse dans CHAQUE batch
        // (idempotente, gasless — le 7 juil., l'approbation « une fois » liée à un
        // premier batch jamais miné a fait reverter tous les merges suivants).
        let mut calls = Vec::new();
        {
            calls.push(Call {
                target: addr(CTF)?,
                value: U256::ZERO,
                data: setApprovalForAllCall {
                    operator: addr(CTF_COLLATERAL_ADAPTER)?,
                    approved: true,
                }
                .abi_encode()
                .into(),
            });
        }
        calls.push(Call {
            target: addr(CTF_COLLATERAL_ADAPTER)?,
            value: U256::ZERO,
            data: adapter_calldata.clone().into(),
        });

        let deadline = chrono::Utc::now().timestamp() as u64 + 300;
        let batch = Batch {
            wallet,
            nonce: U256::from(nonce),
            deadline: U256::from(deadline),
            calls: calls.clone(),
        };
        let domain = eip712_domain! {
            name: "DepositWallet",
            version: "1",
            chain_id: 137,
            verifying_contract: wallet,
        };
        let hash = batch.eip712_signing_hash(&domain);
        let signer: PrivateKeySigner = self
            .creds
            .private_key
            .parse()
            .map_err(|e| anyhow::anyhow!("clé privée: {e}"))?;
        let sig = signer
            .sign_hash_sync(&hash)
            .map_err(|e| anyhow::anyhow!("signature batch: {e}"))?;

        // 3. soumission
        let body = serde_json::json!({
            "type": "WALLET",
            "from": owner,
            "to": DEPOSIT_WALLET_FACTORY,
            "nonce": nonce.to_string(),
            "signature": format!("0x{}", hex_encode(&sig.as_bytes())),
            "depositWalletParams": {
                "depositWallet": self.creds.funder,
                "deadline": deadline.to_string(),
                "calls": calls.iter().map(|c| serde_json::json!({
                    "target": format!("{:#x}", c.target),
                    "value": "0",
                    "data": format!("0x{}", hex_encode(&c.data)),
                })).collect::<Vec<_>>(),
            },
        })
        .to_string();
        let resp = self.relayer_post("/submit", &body).await?;
        let v: serde_json::Value = serde_json::from_str(&resp)
            .map_err(|e| anyhow::anyhow!("submit JSON '{resp}': {e}"))?;
        let tx_id = v
            .get("transactionID")
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("transactionID absent: {resp}"))?
            .to_string();
        tracing::info!(op, tx_id = %tx_id, condition = %condition_id.chars().take(12).collect::<String>(),
            "transaction relayer soumise");

        // 4. suivi de confirmation (task détachée, résultat par oneshot)
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let api_key = self.api_key.clone();
        let api_key_address = self.api_key_address.clone();
        let op = op.to_string();
        tokio::spawn(async move {
            let mut last_state = String::new();
            let mut first_poll = true;
            for _ in 0..75 {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let url = format!("{RELAYER_BASE}/transaction?id={tx_id}"); // param en QUERY (doc) — /transaction/{{id}} = 404
                let Ok(resp) = HTTP
                    .get(&url)
                    .header("RELAYER_API_KEY", &api_key)
                    .header("RELAYER_API_KEY_ADDRESS", &api_key_address)
                    .send()
                    .await
                else {
                    continue;
                };
                let http_status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                if first_poll || !http_status.is_success() {
                    first_poll = false;
                    tracing::info!(op = %op, %http_status,
                        raw = %text.chars().take(300).collect::<String>(),
                        "suivi relayer — réponse brute");
                }
                let state = serde_json::from_str::<serde_json::Value>(&text)
                    .ok()
                    .and_then(|v| {
                        v.get("state")
                            .or_else(|| v.get(0).and_then(|x| x.get("state")))
                            .and_then(|s| s.as_str().map(String::from))
                    })
                    .unwrap_or_default();
                last_state = state.clone();
                match state.as_str() {
                    s if s.contains("CONFIRMED") || s.contains("MINED") || s.contains("EXECUTED") || s.contains("SUCCESS") => {
                        tracing::info!(op = %op, tx_id = %tx_id, "✅ transaction relayer CONFIRMÉE");
                        let _ = done_tx.send(TxOutcome::Confirmed);
                        return;
                    }
                    s if s.contains("FAILED") || s.contains("INVALID") || s.contains("REVERTED") => {
                        tracing::warn!(op = %op, tx_id = %tx_id, resp = %text, "transaction relayer ÉCHOUÉE");
                        let _ = done_tx.send(TxOutcome::Failed(text));
                        return;
                    }
                    _ => {}
                }
            }
            tracing::warn!(op = %op, tx_id = %tx_id, dernier_etat = %last_state,
                "confirmation relayer : timeout de suivi (150 s)");
            let _ = done_tx.send(TxOutcome::Failed(format!("timeout (dernier état: {last_state})")));
        });
        Ok(done_rx)
    }

    async fn relayer_get(&self, path: &str) -> anyhow::Result<String> {
        let resp = HTTP
            .get(format!("{RELAYER_BASE}{path}"))
            .header("RELAYER_API_KEY", &self.api_key)
            .header("RELAYER_API_KEY_ADDRESS", &self.api_key_address)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("relayer GET {path} {status}: {text}");
        }
        Ok(text)
    }

    async fn relayer_post(&self, path: &str, body: &str) -> anyhow::Result<String> {
        let resp = HTTP
            .post(format!("{RELAYER_BASE}{path}"))
            .header("Content-Type", "application/json")
            .header("RELAYER_API_KEY", &self.api_key)
            .header("RELAYER_API_KEY_ADDRESS", &self.api_key_address)
            .body(body.to_string())
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("relayer POST {path} {status}: {text}");
        }
        Ok(text)
    }
}

fn addr(s: &str) -> anyhow::Result<Address> {
    Address::from_str(s).map_err(|e| anyhow::anyhow!("adresse '{s}': {e}"))
}

fn b256(s: &str) -> anyhow::Result<B256> {
    B256::from_str(s).map_err(|e| anyhow::anyhow!("conditionId '{s}': {e}"))
}

fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_calldata_encodes() {
        let data = mergePositionsCall {
            collateralToken: addr(PUSD).unwrap(),
            parentCollectionId: B256::ZERO,
            conditionId: B256::ZERO,
            partition: vec![U256::from(1u8), U256::from(2u8)],
            amount: U256::from(5_000_000u64), // 5 paires
        }
        .abi_encode();
        // sélecteur 4 octets + payload ABI
        assert!(data.len() > 4 + 32 * 5);
    }

    #[test]
    fn batch_signing_hash_is_stable() {
        let wallet = addr("0x00000000000fb5c9adea0298d729a0cb3823cc07").unwrap();
        let batch = Batch {
            wallet,
            nonce: U256::from(7u8),
            deadline: U256::from(1_760_000_000u64),
            calls: vec![Call { target: wallet, value: U256::ZERO, data: vec![0u8, 1u8].into() }],
        };
        let domain = eip712_domain! {
            name: "DepositWallet",
            version: "1",
            chain_id: 137,
            verifying_contract: wallet,
        };
        let h1 = batch.eip712_signing_hash(&domain);
        let h2 = batch.eip712_signing_hash(&domain);
        assert_eq!(h1, h2);
        assert_ne!(h1, B256::ZERO);
    }
}
