-- Migration 068: Expand sanctions screening targets with verified wallet addresses
--
-- All addresses verified against Etherscan public labels.
-- Sources:
--   Tether Treasury:  https://etherscan.io/address/0x5754284f345afc66a98fbb0a0afe71e0f007b949
--   Tether Multisig:  https://etherscan.io/address/0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828
--   Bitfinex Hot:     https://etherscan.io/address/0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec
--   Bitfinex 2:       https://etherscan.io/address/0x742d35cc6634c0532925a3b844bc454e4438f44e
--   Bitfinex 3:       https://etherscan.io/address/0x876eabf441b2ee5b5b0554fd502a8e0600950cfa
--   Bitfinex Multi:   https://etherscan.io/address/0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e
--   Bitfinex Cold:    https://etherscan.io/address/0xf4b51b14b9ee30dc37ec970b50a486f37686e2a8
--   WLF Multisig:     https://etherscan.io/address/0x5be9a4959308a0d0c7bc0870e319314d8d957dbb

-- Tether treasury and operational wallets
INSERT INTO sanctions_screen_targets
    (entity_type, entity_symbol, target_name, target_type)
VALUES
    ('stablecoin_issuer', 'usdt', '0x5754284f345afc66a98fbb0a0afe71e0f007b949', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828', 'wallet_address')
ON CONFLICT DO NOTHING;

-- Bitfinex wallets (iFinex — Tether parent company)
INSERT INTO sanctions_screen_targets
    (entity_type, entity_symbol, target_name, target_type)
VALUES
    ('stablecoin_issuer', 'usdt', '0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0x742d35cc6634c0532925a3b844bc454e4438f44e', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0xf4b51b14b9ee30dc37ec970b50a486f37686e2a8', 'wallet_address')
ON CONFLICT DO NOTHING;

-- World Liberty Financial / USD1 multisig
INSERT INTO sanctions_screen_targets
    (entity_type, entity_symbol, target_name, target_type)
VALUES
    ('stablecoin_issuer', 'usd1', '0x5be9a4959308a0d0c7bc0870e319314d8d957dbb', 'wallet_address')
ON CONFLICT DO NOTHING;


INSERT INTO migrations (name) VALUES ('068_sanctions_wallet_targets') ON CONFLICT DO NOTHING;
