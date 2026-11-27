const express = require('express');
const bookings = require('./bookings');
const webhooks = require('./webhooks');

const app = express();
app.use('/slots', bookings);
app.use('/webhooks', webhooks);
app.listen(4000);
