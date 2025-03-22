import { EuiTitle, EuiInputPopover, EuiSelectable } from "@elastic/eui";

import React, { useEffect, useRef, useState } from "react";
import {
  SuggestionModes,
  TagSuggestionInstance,
} from "../hooks/useSearchInputWithTags";

interface TagSearchInterface {
  currentTag: string;
  tagsString: string;
  setTagsString: (tagsString: string) => void;
  acceptSuggestion: (suggestion: TagSuggestionInstance) => void;
  tagSuggestions: TagSuggestionInstance[];
  suggestionMode: SuggestionModes;
  setCursorPosition: (position: number | undefined) => void;
}

interface SelectableOption {
  label: string;
  checked?: "on" | "off" | undefined;
  suggestion: TagSuggestionInstance;
}

// Helper Functions
const suggestionFormatter = (item: TagSuggestionInstance) => {
  return {
    label: item.suggestion,
    suggestion: item,
    showIcons: false,
    append: <span>{item.description}</span>,
  };
};

const getCursorPosition = (
  inputNode: React.MutableRefObject<HTMLInputElement | null>,
) => {
  return inputNode.current?.selectionStart || undefined;
};

const computePlaceholderText = (
  tagSuggestions: TagSuggestionInstance[] | undefined,
) => {
  return !tagSuggestions
    ? ""
    : "e.g. " +
        tagSuggestions
          .slice(0, 2)
          .map((s) => `"${s.suggestion}"`)
          .join(" or ");
};

const generateResultsCount = (
  currentTag: string,
  suggestionMode: SuggestionModes,
  tagSuggestions: TagSuggestionInstance[],
) => {
  let resultsCount = undefined;

  const currentTagIsEmpty = currentTag.length <= 0;
  const currentTagHasNoValue = currentTag.split(":")[1] === "";
  const operatingWord =
    currentTagIsEmpty || currentTagHasNoValue ? "possible" : "matching";
  const counterWord = suggestionMode === "KEY" ? `key` : `value`;

  if (tagSuggestions.length > 0) {
    const isPlural = tagSuggestions.length > 1 ? "s" : "";
    resultsCount = (
      <span>{`${tagSuggestions.length} ${operatingWord} ${counterWord}${isPlural}`}</span>
    );
  }

  return resultsCount;
};

// Hooks
const useInputHack = (
  setTagsString: (s: string) => void,
  setCursorPosition: (n: number | undefined) => void,
) => {
  // HACK --- route around the lack of onChange
  // See: https://github.com/elastic/eui/issues/5651
  const inputNode = useRef<HTMLInputElement | null>(null);
  useEffect(() => {
    const cb = () => {
      const s: string = inputNode.current?.value || "";

      setTagsString(s);
      setCursorPosition(getCursorPosition(inputNode));
    };

    const copiedNode = inputNode.current;

    if (copiedNode) {
      copiedNode.addEventListener("input", cb);
    }

    return () => {
      if (copiedNode) {
        copiedNode.removeEventListener("input", cb);
      }
    };
  }, [inputNode, setTagsString, setCursorPosition]);

  return inputNode;
};

const useSelectableOptions = (
  tagSuggestions: TagSuggestionInstance[],
  acceptSuggestion: (suggestion: TagSuggestionInstance) => void,
) => {
  const [options, setOptions] = useState<SelectableOption[]>(
    tagSuggestions ? tagSuggestions.map(suggestionFormatter) : [],
  );

  const onSelectableChange = (options: SelectableOption[]) => {
    // Get the thing that just got "checked"
    const clickedItem = options.find((option) => option.checked === "on");

    if (clickedItem) {
      acceptSuggestion(clickedItem.suggestion);
    }

    setOptions(options);
  };

  useEffect(() => {
    // Update options when new set of suggestions are passed down
    setOptions(tagSuggestions.map(suggestionFormatter));
  }, [tagSuggestions, setOptions]);

  return {
    options,
    onSelectableChange,
  };
};

const TagSearch = ({
  currentTag,
  tagsString,
  setTagsString,
  acceptSuggestion,
  tagSuggestions,
  suggestionMode,
  setCursorPosition,
}: TagSearchInterface) => {
  // HACK --- route around the lack of onChange
  const inputNode = useInputHack(setTagsString, setCursorPosition);

  // Handling Suggestion Options
  const { options, onSelectableChange } = useSelectableOptions(
    tagSuggestions, // Gets turned into options
    acceptSuggestion, // Get triggered when an option is selected
  );

  // Using EuiInputPopover: https://elastic.github.io/eui/#/layout/popover
  const [hasFocus, setHasFocus] = useState<boolean>(false);

  // Props for EuiFieldSearch
  const searchProps = {
    value: tagsString,
    inputRef: (node: HTMLInputElement | null) => {
      // HTMLInputElement is hooked into useInputHack
      inputNode.current = node;
    },
    onFocus: () => {
      setHasFocus(true);
    },
    fullWidth: true,
    placeholder: computePlaceholderText(tagSuggestions),
  };

  const resultsCount = generateResultsCount(
    currentTag,
    suggestionMode,
    tagSuggestions,
  );

  return (
    <>
      <EuiTitle size="xs">
        <h2>Filter by Tags</h2>
      </EuiTitle>
      <EuiSelectable
        onFocus={() => {
          setHasFocus(true);
        }}
        onBlur={() => {
          setHasFocus(false);
        }}
        searchable={true}
        isPreFiltered={true}
        searchProps={searchProps}
        aria-label="Filter by "
        onChange={onSelectableChange}
        options={options}
        singleSelection={true}
        listProps={{ bordered: true }}
      >
        {(list, search) => {
          return (
            <EuiInputPopover
              fullWidth
              disableFocusTrap={true}
              input={<>{search}</>}
              isOpen={hasFocus}
              closePopover={() => {
                setHasFocus(false);
              }}
            >
              {resultsCount}
              {list}
            </EuiInputPopover>
          );
        }}
      </EuiSelectable>
    </>
  );
};

export default TagSearch;
