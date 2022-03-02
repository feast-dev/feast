(function () {

	/* global document,window,matchMedia */
	var colorIndexRegExp = /((?:not )?all and )?(\(color-index: *(22|48|70)\))/i;
	var prefersColorSchemeRegExp = /prefers-color-scheme:/i;

	var prefersColorSchemeInit = function prefersColorSchemeInit(initialColorScheme) {
	  var mediaQueryString = '(prefers-color-scheme: dark)';
	  var mediaQueryList = window.matchMedia && matchMedia(mediaQueryString);
	  var hasNativeSupport = mediaQueryList && mediaQueryList.media === mediaQueryString;

	  var mediaQueryListener = function mediaQueryListener() {
	    set(mediaQueryList.matches ? 'dark' : 'light');
	  };

	  var removeListener = function removeListener() {
	    if (mediaQueryList) {
	      mediaQueryList.removeListener(mediaQueryListener);
	    }
	  };

	  var set = function set(colorScheme) {
	    if (colorScheme !== currentColorScheme) {
	      currentColorScheme = colorScheme;

	      if (typeof result.onChange === 'function') {
	        result.onChange();
	      }
	    }

	    [].forEach.call(document.styleSheets || [], function (styleSheet) {
	      // cssRules is a live list. Converting to an Array first.
	      var rules = [];
	      [].forEach.call(styleSheet.cssRules || [], function (cssRule) {
	        rules.push(cssRule);
	      });
	      rules.forEach(function (cssRule) {
	        var colorSchemeMatch = prefersColorSchemeRegExp.test(Object(cssRule.media).mediaText);

	        if (colorSchemeMatch) {
	          var index = [].indexOf.call(cssRule.parentStyleSheet.cssRules, cssRule);
	          cssRule.parentStyleSheet.deleteRule(index);
	        } else {
	          var colorIndexMatch = (Object(cssRule.media).mediaText || '').match(colorIndexRegExp);

	          if (colorIndexMatch) {
	            // Old style which has poor browser support and can't handle complex media queries.
	            cssRule.media.mediaText = ((/^dark$/i.test(colorScheme) ? colorIndexMatch[3] === '48' : /^light$/i.test(colorScheme) ? colorIndexMatch[3] === '70' : colorIndexMatch[3] === '22') ? 'not all and ' : '') + cssRule.media.mediaText.replace(colorIndexRegExp, '$2');
	          } else {
	            // New style which supports complex media queries.
	            var colorDepthMatch = (Object(cssRule.media).mediaText || '').match(/\( *(?:color|max-color): *(48842621|70318723|22511989) *\)/i);

	            if (colorDepthMatch && colorDepthMatch.length > 1) {
	              if (/^dark$/i.test(colorScheme) && (colorDepthMatch[1] === '48842621' || colorDepthMatch[1] === '22511989')) {
	                // No preference or preferred is dark and rule is dark.
	                cssRule.media.mediaText = cssRule.media.mediaText.replace(/\( *color: *(?:48842621|70318723) *\)/i, "(max-color: " + colorDepthMatch[1] + ")");
	              } else if (/^light$/i.test(colorScheme) && (colorDepthMatch[1] === '70318723' || colorDepthMatch[1] === '22511989')) {
	                // No preference or preferred is light and rule is light.
	                cssRule.media.mediaText = cssRule.media.mediaText.replace(/\( *color: *(?:48842621|22511989) *\)/i, "(max-color: " + colorDepthMatch[1] + ")");
	              } else {
	                cssRule.media.mediaText = cssRule.media.mediaText.replace(/\( *max-color: *(?:48842621|70318723|22511989) *\)/i, "(color: " + colorDepthMatch[1] + ")");
	              }
	            }
	          }
	        }
	      });
	    });
	  };

	  var result = Object.defineProperty({
	    hasNativeSupport: hasNativeSupport,
	    removeListener: removeListener
	  }, 'scheme', {
	    get: function get() {
	      return currentColorScheme;
	    },
	    set: set
	  }); // initialize the color scheme using the provided value, the system value, or light

	  var currentColorScheme = initialColorScheme || (mediaQueryList && mediaQueryList.matches ? 'dark' : 'light');
	  set(currentColorScheme); // listen for system changes

	  if (mediaQueryList) {
	    if ('addEventListener' in mediaQueryList) {
	      mediaQueryList.addEventListener('change', mediaQueryListener);
	    } else {
	      mediaQueryList.addListener(mediaQueryListener);
	    }
	  }

	  return result;
	};

	/* global self */
	self.prefersColorSchemeInit = prefersColorSchemeInit; // Legacy : there used to be a rollup config that exposed this function under a different name.

	self.initPrefersColorScheme = prefersColorSchemeInit;

})();
//# sourceMappingURL=browser-global.js.map
