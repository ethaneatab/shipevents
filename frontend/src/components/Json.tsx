export function Json({ value, testId }: { value: unknown; testId?: string }) {
  return <pre className="json" data-testid={testId}>{JSON.stringify(value, null, 2)}</pre>;
}
