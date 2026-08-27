import type { Config } from "tailwindcss";
import { mtConfig } from "@material-tailwind/react";
 
const config: Config = {
  darkMode: false, // or 'media' or 'class'
  theme: {
    extend: {},
  },
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
    "./node_modules/@material-tailwind/react/**/*.{js,ts,jsx,tsx}"
  ],
  plugins: [mtConfig({
    colors: {
      primary: {
        default: '#6028ff',
        dark: '#5216eb',
        light: '#724dff',
        foreground: '#f9fafb'
      }
    }
  })],
};
export default config;
