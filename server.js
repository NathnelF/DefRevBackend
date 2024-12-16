const express = require('express');
const { exec } = require('child_process');
const cors = require('cors');
const path = require('path');


const app = express();
const port = process.env.PORT || 8080;

app.use(cors());

// Define the path to the scripts folder
const scriptsDir = path.join(__dirname, 'scripts');

app.get('/', (req, res) => {
  res.send('Hello World!');
});

app.get('/test-script', (req, res) => {
  const scriptPath = path.join(scriptsDir, 'script.py');
  exec(`python "${scriptPath}"`, (err, stdout, stderr) => {
    if (err) {
      console.error(`Error: ${err.message}`);
      return res.status(500).send(`Error: ${err.message}`);
    }
    if (stderr) {
      console.error(`Stderr: ${stderr}`);
      return res.status(500).send(`Stderr: ${stderr}`);
    }
    console.log(`Stdout: ${stdout}`);
    res.send(`Output: ${stdout}`);
  });
});

app.get('/run-schedule', (req, res) => {
  const scriptPath = path.join(scriptsDir, 'recognition_schedule.py');
  exec(`python "${scriptPath}"`, (err, stdout, stderr) => {
    if (err) {
      console.error(`Error: ${err.message}`);
      return res.status(500).send(`Error: ${err.message}`);
    }
    if (stderr) {
      console.error(`Stderr: ${stderr}`);
      return res.status(500).send(`Stderr: ${stderr}`);
    }
    console.log(`Stdout: ${stdout}`);
    res.send(`Output: ${stdout}`);
  });
});

app.get('/contraction', (req, res) => {
  const scriptPath = path.join(scriptsDir, 'contraction.py');
  exec(`python "${scriptPath}"`, (err, stdout, stderr) => {
    if (err) {
      console.error(`Error: ${err.message}`);
      return res.status(500).send(`Error: ${err.message}`);
    }
    if (stderr) {
      console.error(`Stderr: ${stderr}`);
      return res.status(500).send(`Stderr: ${stderr}`);
    }
    console.log(`Stdout: ${stdout}`);
    res.send(`Output: ${stdout}`);
  });
});

app.get('/expansion', (req, res) => {
  const scriptPath = path.join(scriptsDir, 'expansion.py');
  exec(`python "${scriptPath}"`, (err, stdout, stderr) => {
    if (err) {
      console.error(`Error: ${err.message}`);
      return res.status(500).send(`Error: ${err.message}`);
    }
    if (stderr) {
      console.error(`Stderr: ${stderr}`);
      return res.status(500).send(`Stderr: ${stderr}`);
    }
    console.log(`Stdout: ${stdout}`);
    res.send(`Output: ${stdout}`);
  });
});

app.get('/renewal', (req, res) => {
  const scriptPath = path.join(scriptsDir, 'renewal.py');
  exec(`python "${scriptPath}"`, (err, stdout, stderr) => {
    if (err) {
      console.error(`Error: ${err.message}`);
      return res.status(500).send(`Error: ${err.message}`);
    }
    if (stderr) {
      console.error(`Stderr: ${stderr}`);
      return res.status(500).send(`Stderr: ${stderr}`);
    }
    console.log(`Stdout: ${stdout}`);
    res.send(`Output: ${stdout}`);
  });
});


app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});