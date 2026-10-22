import { useEffect, useState } from "react";

const useDocumentTitle = (title: string) => {
  const [document_title, set] = useState(title);
  useEffect(() => {
    document.title = document_title;
  }, [document_title]);

  const setDoucmentTitle = (newTitle: string) => {
    if (document_title !== newTitle) {
      set(newTitle);
    }
  };

  return setDoucmentTitle;
};

export { useDocumentTitle };
