import type { IApiQuery, IPublicQuery } from "./types";

const DEFAULT_LIMIT = 10;
const PER_PAGE = [10, 25, 50, 100];
const PUBLIC_PARAMS = ["q", "page", "limit", "order_by", "schema", "country", "dataset"]; // allowed user facing url params

export const cleanQuery = (
  query: IApiQuery,
  keys: string[] = []
): IApiQuery => {
  if (!!query.limit) {
    // ensure limit is within PER_PAGE
    query.limit =
      PER_PAGE.indexOf(query.limit) < 0 ? DEFAULT_LIMIT : query.limit;
  }
  // filter out empty params and optional filter for specific keys
  return Object.fromEntries(
    Object.entries(query).filter(
      ([k, v]) =>
        (keys.length ? keys.indexOf(k) > -1 : true) &&
        !(v === undefined || v === "")
    )
  );
};

export const getPublicQuery = (query: IApiQuery): IPublicQuery =>
  cleanQuery(query, PUBLIC_PARAMS);
