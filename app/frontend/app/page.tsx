import { AuthButton } from "@/components/auth-button";
import Link from "next/link";

export default function Home() {
  return (
    <main className="min-h-screen flex flex-col items-center">
      <nav className="w-full flex justify-center border-b border-b-foreground/10 h-16">
        <div className="w-full max-w-5xl flex justify-between items-center p-3 px-5 text-sm">
          <Link href="/" className="font-semibold">
            Papaya
          </Link>
          <AuthButton />
        </div>
      </nav>

      <section className="flex-1 flex flex-col items-center justify-center text-center gap-4 p-10">
        <h1 className="text-4xl font-bold">Welcome to Papaya 🍈</h1>
        <p className="text-muted-foreground text-lg max-w-xl">
          This is your streamlined starting point. Supabase is connected, auth
          is working, and you can begin building your app. Yay!
        </p>
        <Link
          href="/instruments"
          className="bg-black text-white py-2 px-4 rounded hover:bg-gray-800"
        >
          View Instruments
        </Link>
      </section>

      <footer className="w-full text-center text-xs py-6 border-t">
        Built with Next.js & Supabase
      </footer>
    </main>
  );
}
