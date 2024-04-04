const fs = require("fs");
const { compileFromFile } = require("json-schema-to-typescript");

fs.readdirSync(__dirname)
  .filter((fn) => fn.indexOf(".json") > 0)
  .map((fn) => {
    compileFromFile(`${__dirname}/${fn}`).then((ts) =>
      fs.writeFileSync(`${__dirname}/${fn.replace(".json", ".d.ts")}`, ts)
    );
  });
