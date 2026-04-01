import Nav from "@/components/Nav";
import Footer from "@/components/Footer";

export default function DocsLayout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <Nav />
      <main className="mx-auto max-w-3xl px-6 pb-24 pt-28">
        <article
          className="prose prose-invert prose-sm max-w-none"
          style={{
            "--tw-prose-headings": "#fff",
            "--tw-prose-body": "#d1d5db",
            "--tw-prose-links": "#818cf8",
          } as React.CSSProperties}
        >
          {children}
        </article>
      </main>
      <Footer />
    </>
  );
}
