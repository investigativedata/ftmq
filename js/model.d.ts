import type { Entity, IEntityDatum } from "./model/entity";

// entity type is mostly used as a union type of the class instance or the data object
export type TEntity = Entity | IEntityDatum;
export type { Entity, IEntityDatum };

// base props interface for components
export interface IEntityComponent {
  entity: TEntity;
}

export type TDatasetCategory =
  | (
      | "news"
      | "leak"
      | "land"
      | "gazette"
      | "court"
      | "company"
      | "sanctions"
      | "procurement"
      | "finance"
      | "grey"
      | "library"
      | "license"
      | "regulatory"
      | "poi"
      | "customs"
      | "census"
      | "transport"
      | "casefile"
      | "other"
    )
  | null;

export type TDatasetFrequency =
  | (
      | "unknown"
      | "never"
      | "hourly"
      | "daily"
      | "weekly"
      | "monthly"
      | "annually"
    )
  | null;

export interface IPublisher {
  name: string;
  url?: string | null;
  description?: string | null;
  country?: string | null;
  country_label?: string | null;
  official?: boolean | null;
  logo_url?: string | null;
}

export interface ICoverage {
  start?: string | null;
  end?: string | null;
  frequency?: Frequency;
  countries?: Countries;
  schedule?: Schedule;
}

export interface ISchemataStats {
  total?: number | null;
  countries?: ICountry[] | null;
  schemata?: ISchema[] | null;
}

export interface ICountry {
  code: string;
  count: number;
  label?: string | null;
}

export interface ISchema {
  name: string;
  count: number;
  label: string | null;
  plural: string | null;
}

export interface IResource {
  name: string;
  url: string;
  title?: string | null;
  checksum?: string | null;
  timestamp?: string | null;
  mime_type?: string | null;
  mime_type_label?: string | null;
  size?: number | null;
}

export interface IMaintainer {
  name: string;
  description?: string | null;
  url?: string | null;
  logo_url?: string | null;
}

export interface IDataset {
  name: string;
  prefix?: string | null;
  title?: string | null;
  license?: string | null;
  summary?: string | null;
  description?: string | null;
  url?: string | null;
  updated_at?: string | null;
  version?: string | null;
  category?: IDatasetCategory;
  publisher?: IPublisher | null;
  coverage?: ICoverage | null;
  things?: ISchemata | null;
  intervals?: ISchemata | null;
  entity_count?: number | null;
  resources?: IResource[] | null;
  index_url?: string | null;
  catalog?: string | null;
  countries?: string[] | null;
  info_url?: string | null;
  data_url?: string | null;
  git_repo?: string | null;
  uri?: string | null;
  maintainer?: IMaintainer | null;
}
