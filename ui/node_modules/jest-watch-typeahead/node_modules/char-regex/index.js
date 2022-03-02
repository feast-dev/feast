// Based on https://github.com/lodash/lodash/blob/6018350ac10d5ce6a5b7db625140b82aeab804df/.internal/unicodeSize.js

export default function charRegex() {
	// Used to compose unicode character classes.
	const astralRange = "\\ud800-\\udfff"
	const comboMarksRange = "\\u0300-\\u036f"
	const comboHalfMarksRange = "\\ufe20-\\ufe2f"
	const comboSymbolsRange = "\\u20d0-\\u20ff"
	const comboMarksExtendedRange = "\\u1ab0-\\u1aff"
	const comboMarksSupplementRange = "\\u1dc0-\\u1dff"
	const comboRange = comboMarksRange + comboHalfMarksRange + comboSymbolsRange + comboMarksExtendedRange + comboMarksSupplementRange
	const varRange = "\\ufe0e\\ufe0f"

	// Used to compose unicode capture groups.
	const astral = `[${astralRange}]`
	const combo = `[${comboRange}]`
	const fitz = "\\ud83c[\\udffb-\\udfff]"
	const modifier = `(?:${combo}|${fitz})`
	const nonAstral = `[^${astralRange}]`
	const regional = "(?:\\ud83c[\\udde6-\\uddff]){2}"
	const surrogatePair = "[\\ud800-\\udbff][\\udc00-\\udfff]"
	const zeroWidthJoiner = "\\u200d"
	const blackFlag = "(?:\\ud83c\\udff4\\udb40\\udc67\\udb40\\udc62\\udb40(?:\\udc65|\\udc73|\\udc77)\\udb40(?:\\udc6e|\\udc63|\\udc6c)\\udb40(?:\\udc67|\\udc74|\\udc73)\\udb40\\udc7f)"

	// Used to compose unicode regexes.
	const optModifier = `${modifier}?`
	const optVar = `[${varRange}]?`
	const optJoin = `(?:${zeroWidthJoiner}(?:${[nonAstral, regional, surrogatePair].join("|")})${optVar + optModifier})*`
	const seq = optVar + optModifier + optJoin
	const nonAstralCombo = `${nonAstral}${combo}?`
	const symbol = `(?:${[blackFlag, nonAstralCombo, combo, regional, surrogatePair, astral].join("|")})`

	// Used to match [string symbols](https://mathiasbynens.be/notes/javascript-unicode).
	return new RegExp(`${fitz}(?=${fitz})|${symbol + seq}`, "g")
}
