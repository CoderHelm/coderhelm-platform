"use client";

import { useState } from "react";
import Link from "next/link";

export default function Nav() {
  const [open, setOpen] = useState(false);

  return (
    <nav className="fixed inset-x-0 top-0 z-50 border-b border-white/10 bg-black/80 backdrop-blur">
      <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
        <Link href="/" className="text-lg font-semibold text-white">
          Coderhelm
        </Link>

        {/* Desktop menu */}
        <div className="hidden items-center gap-6 md:flex">
          <a href="/#how-it-works" className="text-sm text-gray-300 hover:text-white">
            How it works
          </a>
          <Link href="/docs" className="text-sm text-gray-300 hover:text-white">
            Docs
          </Link>
          <Link href="/login" className="text-sm text-gray-300 hover:text-white">
            Login
          </Link>
        </div>

        {/* Mobile hamburger */}
        <button
          type="button"
          className="text-gray-300 md:hidden"
          onClick={() => setOpen(!open)}
          aria-label="Toggle menu"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="h-6 w-6"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            {open ? (
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            ) : (
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            )}
          </svg>
        </button>
      </div>

      {/* Mobile menu */}
      {open && (
        <div className="border-t border-white/10 px-6 pb-4 md:hidden">
          <a
            href="/#how-it-works"
            className="block py-2 text-sm text-gray-300 hover:text-white"
            onClick={() => setOpen(false)}
          >
            How it works
          </a>
          <Link
            href="/docs"
            className="block py-2 text-sm text-gray-300 hover:text-white"
            onClick={() => setOpen(false)}
          >
            Docs
          </Link>
          <Link
            href="/login"
            className="block py-2 text-sm text-gray-300 hover:text-white"
            onClick={() => setOpen(false)}
          >
            Login
          </Link>
        </div>
      )}
    </nav>
  );
}
