import { StringParam } from "use-query-params";
import { encodeQueryParams } from "serialize-query-params";
import { stringify } from "query-string";

const encodeSearchQueryString = (query: string) => {
  return stringify(
    encodeQueryParams(
      {
        tags: StringParam,
      },
      {
        tags: query,
      }
    )
  );
};

export { encodeSearchQueryString };
