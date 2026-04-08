-- Add tool_tags column for inferred drilling tooling needs
ALTER TABLE leads ADD COLUMN tool_tags JSONB DEFAULT '[]'::jsonb;
CREATE INDEX idx_leads_tool_tags ON leads USING GIN (tool_tags);
