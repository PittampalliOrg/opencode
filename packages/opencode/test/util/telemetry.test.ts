import { expect, test } from "bun:test"
import { Telemetry } from "../../src/util/telemetry"

test("builds AI SDK telemetry payload with function id and filtered metadata", () => {
  const result = Telemetry.ai({
    enabled: true,
    functionId: "session.stream",
    metadata: {
      userId: "user-1",
      sessionId: "session-1",
      empty: undefined,
      nullable: null,
      enabled: false,
    },
  })

  expect(result).toEqual({
    experimental_telemetry: {
      isEnabled: true,
      functionId: "session.stream",
      metadata: {
        userId: "user-1",
        sessionId: "session-1",
        enabled: false,
      },
    },
  })
})
