import { defineConfig } from "tsup";

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/storage/index.ts",
    "src/storage/memory.ts",
    "src/storage/d1.ts",
    "src/storage/kv.ts",
    "src/storage/r2.ts",
    "src/storage/sqlite.ts",
  ],
  format: ["esm"],
  dts: true,
  clean: true,
  outDir: "dist",
  noExternal: ["effect"],
});
