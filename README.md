# kyotei-advisor-stable

## Environment Variables

Set these before starting the app or deploying:

- `USE_SUPABASE=True`
- `SUPABASE_URL=https://rngzcwztmshadaevaxqz.supabase.co`
- `SUPABASE_KEY=<your Supabase publishable or anon key>`

For local setup, copy `.env.example` to `.env` and fill in the key.

For Render deployment, `render.yaml` now expects:

- `SUPABASE_URL`
- `SUPABASE_KEY`
