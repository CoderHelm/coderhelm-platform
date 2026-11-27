export function RoomLayout({ slug, children }: { slug: string; children: React.ReactNode }) {
  return <section data-room={slug}>{children}</section>;
}
