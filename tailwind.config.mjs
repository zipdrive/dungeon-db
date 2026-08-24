const withMT = require("@material-tailwind/react/utils/withMT");
 
module.exports = withMT({
  purge: [],
  darkMode: false, // or 'media' or 'class'
  theme: {
    extend: {},
  },
  variants: {
    extend: {},
  },
  plugins: [],
  safelist: [
    'indent-3',
    'indent-6',
    'indent-9',
    'indent-12',
    'indent-15',
    'indent-18',
    'indent-21',
    'indent-24',
    'indent-27',
    'indent-30'
  ]
});