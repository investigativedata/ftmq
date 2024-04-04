// models from `ftmq`
export type { Catalog as ICatalog } from "./Catalog";
export type { Dataset as IDataset } from "./Dataset";
export type { DatasetStats as IDatasetStats } from "./DatasetStats";
export type { Maintainer as IMaintainer } from "./Maintainer";
export type { Publisher as IPublisher } from "./Publisher";
export type { Resource as IResource } from "./Resource";

import type { Entity, IEntityDatum } from "../model/entity";

// entity type is mostly used as a union type of the class instance or the data object
export type TEntity = Entity | IEntityDatum;
export type { Entity, IEntityDatum };

// base props interface for components
export interface IEntityComponent {
  entity: TEntity;
}
