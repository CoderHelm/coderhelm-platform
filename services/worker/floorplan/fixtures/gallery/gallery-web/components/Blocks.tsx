import { Hero } from './blocks/Hero';
import { ArtworkGrid } from './blocks/ArtworkGrid';

type Block = { blockType: string };

/** The page builder: one block component per block in the content. */
export function Blocks({ blocks }: { blocks: unknown[] }) {
  return (
    <>
      {(blocks as Block[]).map((b, i) => {
        switch (b.blockType) {
          case 'hero':
            return <Hero key={i} />;
          case 'artworks':
            return <ArtworkGrid key={i} />;
          default:
            return null;
        }
      })}
    </>
  );
}
