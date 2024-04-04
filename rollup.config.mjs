import typescript from "rollup-plugin-typescript2";

export default {
  input: "js/index.ts",
  output: [
    {
      file: "dist/index.js",
      format: "cjs",
      exports: "named",
      sourcemap: true,
      strict: false,
    },
  ],
  plugins: [typescript()],
  external: ["query-string"],
};
