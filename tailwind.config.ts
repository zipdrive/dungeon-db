import type { Config } from "tailwindcss";
import { mtConfig } from "@material-tailwind/react";
 
const config: Config = {
  darkMode: false, // or 'media' or 'class'
  theme: {
    extend: {},
  },
  content: [
    "./index.html",
    "./node_modules/@material-tailwind/react/**/*.{js,ts,jsx,tsx}"
  ],
  plugins: [mtConfig],
};
export default config;
