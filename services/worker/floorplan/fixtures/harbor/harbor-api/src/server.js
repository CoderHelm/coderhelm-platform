const express = require('express');
const bookings = require('./bookings');

const app = express();
app.use('/slips', bookings);
app.listen(4000);
