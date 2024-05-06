import pino from "pino";

const { KELPIE_LOG_LEVEL = "info" } = process.env;

export const log = pino(
  {
    name: "kelpie",
    level: KELPIE_LOG_LEVEL,
    formatters: {
      level(label, number) {
        return { level: label };
      },
    },
  },
  pino.destination({
    sync: false,
  })
);
