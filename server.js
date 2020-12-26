const csv = require('csv-parser');
const cliProgress = require('cli-progress');
const exec = require('child_process').execSync;
const Spinner = require('cli-spinner').Spinner;

fs = require('fs')
const readline = require('readline');
const mongoose = require('mongoose');

const uri = 'mongodb://localhost:27017/jpTatoeba'

mongoose.connect(uri, { useUnifiedTopology: true, useNewUrlParser: true })

const connection = mongoose.connection

const Schema = mongoose.Schema;

const ObjectId = Schema.ObjectId;

const Sentence = new Schema({
  sentenceId: ObjectId,
  en: String,
  jp: String,
});

const Sentences = mongoose.model('sentences', Sentence);

connection.once('open', async () => {
  console.log("MongoDB database connection established successfully");

  const getFileLen = (path) => {
    const wc = exec(`wc -l ${path}`).toString();
    return wc.match(/[0-9]+/)[0];
  }

  const buildNewBar = (msg, path) => {
    const barOpt = () => ({
      format: `${msg} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}`,
      stopOnComplete: true,
    })
    const bar = new cliProgress.SingleBar(barOpt(), cliProgress.Presets.shades_classic)
    const len = getFileLen(path);
    const startBar = () => bar.start(len, 0);

    return { bar, startBar };
  }

  let corpuses = {};

  let spinner = new Spinner('Creating Database.. %s');
  spinner.setSpinnerString('|/-\\');

  const flipObject = (obj) => (
    Object.keys(obj).reduce((acc, key) => {
      acc[obj[key]] = key;
      return acc;
    }, {})
  );

  const buildData = (obj) => Object.keys(obj).map(key => ({ en: key, jp: obj[key] }));

  const handleSentences = (line, type) => {
    const reg = new RegExp(`([0-9]*)\\s+${type}\\s+(.*)`)
    const parsed = line.match(reg, 'gm');
    const [_, index, value] = parsed || [];

    if (index && value && corpuses[index]) {
      corpuses[value] = corpuses[index];
      delete corpuses[index];
    }
  }

  const handleIndices = (line) => {
    const parsed = line.match(/^([0-9]*)\t([0-9]*).*$/)
    const [_, jp, en] = parsed || [];
    if (jp && en) {
      corpuses[jp] = en;
    }
  }

  const processLineByLine = async (type, path, msg) => {
    try {
      const { bar, startBar } = buildNewBar(`${msg}: \t`, path);
      startBar();

      let timer;
      const fileStream = fs.createReadStream(path);

      if (type === 'csv') fileStream.pipe(csv());

      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      let i = 0;
      await (async () => {
        for await (const line of rl) {
          type === 'csv'
            ? handleIndices(line)
            : handleSentences(line, type)
          i++;
          bar.increment();
        }
      })();

      clearInterval(timer);

      if (type === 'eng') {
        spinner.start();
      } else if (type === 'csv') {
        return Promise.resolve();
      }

      return Promise.resolve(type === 'jpn' ? flipObject(corpuses) : buildData(corpuses));
    } catch (err) {
      return Promise.reject(err);
    }
  }

  try {
    await processLineByLine('csv', './jpn_indices/jpn_indices.csv', 'Getting indexes');
    corpuses = await processLineByLine('jpn', './jpn_sentences/jpn_sentences.tsv', 'Getting eng sentences');
    const rawData = await processLineByLine('eng', './eng_sentences/eng_sentences.tsv', 'Getting jpn sentences');
    const data = rawData.filter(({ jp, en }) => jp && en && isNaN(jp) && isNaN(en));
    await Sentences.insertMany(data).finally(() => spinner.stop());
  } catch (err) {
    console.log(`\n\u274C ${err}`);
  }
  console.log('\n\u2714 Everything went well');
  process.exit();
})
