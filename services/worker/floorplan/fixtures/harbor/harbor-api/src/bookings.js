const express = require('express');
const { charge } = require('./billing');

const router = express.Router();
router.post('/:id/bookings', async (req, res) => {
  await charge(req.params.id);
  res.json({ ok: true });
});

module.exports = router;
