module.exports = {
  extends: ["react-app", "react-app/jest"],
  overrides: [
    {
      files: ["./scripts/**", "./config/**"],
      parserOptions: { sourceType: "script" },
      rules: { strict: "off" },
    },
  ],
};
