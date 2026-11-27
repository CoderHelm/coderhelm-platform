import { createClient } from 'contentful';

export const cms = createClient({ space: 'gallery-space', accessToken: 'made-up-token' });
