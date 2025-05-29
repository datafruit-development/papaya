import type { NextConfig } from "next";
import dotenv from "dotenv";
dotenv.config({ path: "../.env" });

const nextConfig: NextConfig = {
  /* config options here */
  env: {
    NEXT_PUBLIC_SUPABASE_URL: process.env.SUPABASE_URL,
    NEXT_PUBLIC_SUPABASE_ANON_KEY: process.env.SUPABASE_KEY,
  },
};

export default nextConfig;
