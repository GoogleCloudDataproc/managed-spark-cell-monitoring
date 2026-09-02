/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'jsdom',
  setupFilesAfterEnv: ['<rootDir>/src/setupTests.ts'],
  moduleNameMapper: {
    '\\.(css|less|scss|sass)$': 'identity-obj-proxy'
  },
  transformIgnorePatterns: [
    "node_modules/(?!(pretty-ms|parse-ms|vis-timeline|vis-data|vis-util)/)"
  ],
  transform: {
    "^.+\\.(t|j)sx?$": ["ts-jest", {
      tsconfig: "tsconfig.json"
    }]
  }
};
