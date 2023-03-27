const fs = require("fs"); 
const readline = require("readline"); 
const MaxMem = 500 * 1024 * 1024; // максимальный размер блока в байтах
const ChunkSize = 100000; // размер части файла, который будет обработан за раз

//функция для сортировки массива строк. 
async function sortChunk(chunk) {
  return chunk.sort();
}

//функция для слияния отсортированных частей файлов в один отсортированный файл.
async function mergeChunks(chunks, output) {
  const readers = [];
  const rlOptions = {
    terminal: false,
    crlfDelay: Infinity,
  };

  for (let i = 0; i < chunks.length; i++) {
    
    const stream = fs.createReadStream(chunks[i], { encoding: "utf-8" });
    readers.push(readline.createInterface({ input: stream, ...rlOptions }));
  }

  // текущие значения из каждого чанка
  const values = [];

  // получаем следующее значение из каждого чанка (readLine)
  for (let i = 0; i < readers.length; ++i) {
    const reader = readers[i];
    reader[Symbol.asyncIterator]()
      .next()
      .then((data) => {
        values[i] = data;
      });
  }

  const writeLine = async (line) => {
    const hasSpace = await output.write(`${line}\n`, "utf-8");
    return hasSpace;
  };

 
  let canRead = true;
  while (canRead) {
    let min = values[0];
    let minIndex = 0;
    for (let i = 0; i < values.length; ++i) {
      if (
        values[i] &&
        (min === undefined || (values[i] !== undefined && min > values[i]))
      ) {
        min = values[i];
        minIndex = i;
      }
    }
    if (min !== undefined) {
      const moved = await writeLine(min);
      if (moved) {
        const reader = readers[minIndex];
        const { done, value } = await reader[Symbol.asyncIterator]().next();
        if (done) {
          values[minIndex] = undefined;
        } else {
          values[minIndex] = value;
        }
      } else {
        canRead = false;
      }
    } else {
      canRead = false;
    }
  }
  await output.end();
}
//основная функция для сортировки файла. 
async function sortFile(filename) {
  const stats = await fs.promises.stat(filename); //получаем размер файла.
  const fileSize = stats.size;
  const readStream = fs.createReadStream(filename, { highWaterMark: MAX_MEM });
  let chunkIndex = 1;
  const chunks = [];

  //создаем интерфейс для построчного чтения файла. 
  const rl = readline.createInterface({
    input: readStream,
    terminal: false,
    crlfDelay: Infinity,
  });

  let currentChunk = []; //создаем пустой массив для текущей части файла.

  //событие для обработки каждой строки файла.
  rl.on("line", (line) => {
    currentChunk.push(line);
    if (Buffer.byteLength(currentChunk.join("\n"), "utf8") >= CHUNK_SIZE) {
      const sortedChunk = sortChunk(currentChunk); 
      const chunkFilename = `${filename}.chunk.${chunkIndex}`; //создаем уникальное имя файла для текущей части файла. 
      chunks.push(chunkFilename); //добавляем имя файла текущей части файла в массив всех частей файла. 
       
      const writeStream = fs.createWriteStream(chunkFilename, {
        encoding: "utf-8",
      });
      //записываем отсортированные строки в текущую часть файла.
      sortedChunk.forEach((sortedLine) => {
        writeStream.write(sortedLine);
        writeStream.write("\n");
      });
      writeStream.end();  
      chunkIndex += 1; // увеличиваем индекс части файла
      currentChunk = []; 
    }
  });
  //событие для закрытия чтения файла после обработки всех строк.
  rl.on("close", async () => {
    if (currentChunk.length > 0) {
      const sortedChunk = sortChunk(currentChunk);
      const chunkFilename = `${filename}.chunk.${chunkIndex}`;
      chunks.push(chunkFilename);
      const writeStream = fs.createWriteStream(chunkFilename, {
        encoding: "utf-8",
      });
      sortedChunk.forEach((sortedLine) => {
        writeStream.write(sortedLine);
        writeStream.write("\n");
      });
      writeStream.end(); //закрываем поток записи текущей части файла.
    }
    const output = fs.createWriteStream(`${filename}.sorted`, {
      encoding: "utf-8", //создаем поток записи для отсортированного файла.
    });
    await mergeChunks(chunks, output); //вызываем функцию для слияния отсортированных частей файла в один отсортированный файл. 
    chunks.forEach((chunk) => {
      fs.unlinkSync(chunk);
    });
  });
}



