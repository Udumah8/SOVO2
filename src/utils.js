/**
 * Generates a random number within a specified range.
 * @param {number} min - The minimum value (inclusive).
 * @param {number} max - The maximum value (inclusive).
 * @returns {number} A random number between min and max.
 */
export function getRandomNumberBetween(min, max) {
  return Math.random() * (max - min) + min;
}

/**
 * Generates a random integer within a specified range.
 * @param {number} min - The minimum value (inclusive).
 * @param {number} max - The maximum value (inclusive).
 * @returns {number} A random integer between min and max.
 */
export function getRandomIntegerBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Applies a percentage-based jitter to a base value.
 * @param {number} base - The base value.
 * @param {number} jitterPct - The jitter percentage (e.g., 0.1 for 10%).
 * @returns {number} The jittered value.
 */
export function getJitteredValue(base, jitterPct) {
  const min = base * (1 - jitterPct);
  const max = base * (1 + jitterPct);
  return getRandomNumberBetween(min, max);
}