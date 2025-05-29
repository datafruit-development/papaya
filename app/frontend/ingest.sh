#!/bin/bash

# Ingest only the meaningful custom logic from Papaya frontend
gitingest -o papaya_digest.txt \
  -e "node_modules/,dist/,build/,__pycache__/,*.pyc,*.class,*.log,.next/,.vercel/,public/,*.css,.env.local,*.lock,*.map,*.svg,*.png,*.jpg,*.jpeg,components/ui/,components/*form.tsx,components/auth-button.tsx,components/logout-button.tsx,components/next-logo.tsx,components/supabase-logo.tsx,components/theme-switcher.tsx,app/auth/,globals.css,*.zip,*.mjs" \
  && cat papaya_digest.txt | pbcopy

rm papaya_digest.txt
