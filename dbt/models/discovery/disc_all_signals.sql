-- Union of all domain signal models, sorted by novelty
SELECT * FROM {{ ref('disc_sii_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_wallet_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_psi_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_cqi_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_witness_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_pulse_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_divergence_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_attestation_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_actor_signals') }}
UNION ALL
SELECT * FROM {{ ref('disc_cqi_contagion_signals') }}
