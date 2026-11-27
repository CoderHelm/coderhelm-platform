export async function POST(req: Request) {
  const body = await req.json();
  return Response.json({ booked: true, slug: body.slug });
}
