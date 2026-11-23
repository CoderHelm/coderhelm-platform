const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

const s3 = new S3Client({});

exports.handler = async (event) => {
  const key = event.Records[0].s3.object.key;
  await s3.send(new GetObjectCommand({ Bucket: process.env.RECEIPTS_BUCKET, Key: key }));
  return { ok: true };
};
