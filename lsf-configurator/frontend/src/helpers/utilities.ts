export const generateComponentColor = (
  component: string,
  saturation: number = 65,
  lightness: number = 55
) => {
  // Simple but stable hash → integer
  let hash = 0;
  for (let i = 0; i < component.length; i++) {
    hash = component.charCodeAt(i) + ((hash << 5) - hash);
  }

  // Golden angle (~137.5°) ensures evenly spaced hues
  const goldenAngle = 137.508;
  const hue = (Math.abs(hash) * goldenAngle) % 360;

  return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
};

export function toSnakeCase(str: string) {
  return str
    .replace(/([A-Z])/g, "_$1") // insert _ before capitals
    .toLowerCase();
}

export function keysToSnakeCase(obj: object): object {
  if (Array.isArray(obj)) {
    return obj.map(keysToSnakeCase);
  } else if (obj !== null && typeof obj === "object") {
    return Object.fromEntries(
      Object.entries(obj).map(([k, v]) => [toSnakeCase(k), keysToSnakeCase(v)])
    );
  }
  return obj;
}
