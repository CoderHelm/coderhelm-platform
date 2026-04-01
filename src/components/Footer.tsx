import Link from "next/link";

export default function Footer() {
  return (
    <footer className="border-t border-white/10 bg-black">
      <div className="mx-auto grid max-w-6xl grid-cols-2 gap-8 px-6 py-12 sm:grid-cols-4">
        <div>
          <h3 className="text-sm font-semibold text-white">Product</h3>
          <ul className="mt-4 space-y-2 text-sm text-gray-400">
            <li>
              <a href="/#how-it-works" className="hover:text-white">
                How it works
              </a>
            </li>
            <li>
              <Link href="/docs" className="hover:text-white">
                Docs
              </Link>
            </li>
          </ul>
        </div>
      </div>
      <div className="border-t border-white/10 px-6 py-6 text-center text-xs text-gray-500">
        &copy; {new Date().getFullYear()} Coderhelm. All rights reserved.
      </div>
    </footer>
  );
}
