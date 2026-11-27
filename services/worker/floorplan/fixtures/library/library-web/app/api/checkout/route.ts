export async function POST(req: Request) {
  const body = await req.json();
  return Response.json({ paid: true, slug: body.slug });
}
