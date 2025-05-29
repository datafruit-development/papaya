import { createClient } from "@/lib/supabase/server";

export default async function Instruments() {
  const supabase = await createClient();
  const { data: user } = await supabase.auth.getUser();
  console.log("Current user:", user);
  const { data: instruments, error } = await supabase
    .from("instruments")
    .select();
  console.log({ instruments, error });

  return <pre>{JSON.stringify(instruments, null, 2)}</pre>;
}
