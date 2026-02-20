export namespace Telemetry {
  type Value = string | number | boolean | undefined | null

  type Metadata = Record<string, Value>

  function metadata(input?: Metadata) {
    if (!input) return {}
    return Object.fromEntries(
      Object.entries(input).flatMap(([key, value]) => {
        if (value === undefined || value === null) return []
        return [[key, value]]
      }),
    )
  }

  export function ai(input: {
    enabled?: boolean
    functionId: string
    metadata?: Metadata
  }) {
    return {
      experimental_telemetry: {
        isEnabled: input.enabled,
        functionId: input.functionId,
        metadata: metadata(input.metadata),
      },
    } as const
  }
}
