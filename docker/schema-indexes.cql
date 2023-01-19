CREATE INDEX IF NOT EXISTS "node_headers__parent_id" ON uexplorer.node_headers (parent_id);
CREATE INDEX IF NOT EXISTS "node_headers__ts" ON uexplorer.node_headers (timestamp);

CREATE INDEX IF NOT EXISTS "node_transactions__tx_id" ON uexplorer.node_transactions (tx_id);
CREATE INDEX IF NOT EXISTS "node_transactions__timestamp" ON uexplorer.node_transactions (timestamp);
CREATE INDEX IF NOT EXISTS "node_transactions__inclusion_height" ON uexplorer.node_transactions (inclusion_height);

CREATE INDEX IF NOT EXISTS "node_inputs__box_id" ON uexplorer.node_inputs (box_id);
CREATE INDEX IF NOT EXISTS "node_inputs__tx_id" ON uexplorer.node_inputs (tx_id);

CREATE INDEX IF NOT EXISTS "node_outputs__tx_id" ON uexplorer.node_outputs (tx_id);
CREATE INDEX IF NOT EXISTS "node_outputs__timestamp" ON uexplorer.node_outputs (timestamp);

CREATE INDEX IF NOT EXISTS "node_assets__box_id" ON uexplorer.node_assets (box_id);
CREATE INDEX IF NOT EXISTS "node_assets__token_id" ON uexplorer.node_assets (token_id);

CREATE INDEX IF NOT EXISTS "node_registers__box_id" ON uexplorer.node_registers (box_id);

CREATE INDEX IF NOT EXISTS "node_tokens__box_id" ON uexplorer.node_tokens (box_id);
