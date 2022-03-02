/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * This function calculates the initials/acronym for a given name.
 * It defaults to only 2 characters and will take the first character (of each word).
 * If only one word is supplied for the name, it will only pass back the first letter of the word,
 * unless forced to 2 letters by setting `initialsLength` to `2`.
 * It will pass back the characters with the same casing as the original string
 * unless otherwise specified.
 *
 * @param {string} name The full name of the item to turn into initials
 * @param {number} initialsLength (Optional) How many characters to show (max 2 allowed)
 * @param {string} initials (Optional) Custom initials (max 2 characters)
 * @returns {string} True if the color is dark, false otherwise.
 */
export var MAX_INITIALS = 2;
export function toInitials(name, initialsLength, initials) {
  // Calculate the number of initials to show, maxing out at MAX_INITIALS
  var calculatedInitialsLength = initials ? initials.split(' ').length : name.split(' ').length;
  calculatedInitialsLength = calculatedInitialsLength > MAX_INITIALS ? MAX_INITIALS : calculatedInitialsLength; // Check if initialsLength was passed and set to calculated, unless greater than MAX_INITIALS

  if (initialsLength) {
    calculatedInitialsLength = initialsLength <= MAX_INITIALS ? initialsLength : MAX_INITIALS;
  }

  var calculatedInitials; // A. Set to initials prop if exists (but truncate to 2 characters max unless length is supplied)

  if (initials) {
    calculatedInitials = initials.substring(0, calculatedInitialsLength);
  } else {
    if (name.trim() && name.split(' ').length > 1) {
      // B. If there are any spaces in the name, set to first letter of each word
      calculatedInitials = name.match(/\b(\w)/g);
      calculatedInitials = calculatedInitials && calculatedInitials.join('').substring(0, calculatedInitialsLength);
    } else {
      // C. Set to the name's initials truncated based on calculated length
      calculatedInitials = name.substring(0, calculatedInitialsLength);
    }
  }

  return calculatedInitials;
}