import icons from "../data/icons.json";
import type { Schema } from "./schema";

interface IIconStorage {
  [iconName: string]: string[];
}

const IconRegistry = {
  SIZE: 24,
  storage: icons as IIconStorage,

  getIcon(iconName: string): string[] {
    return this.storage[iconName];
  },

  getSchemaIcon(schema: Schema): string[] {
    const iconName = schema.name.toLowerCase();
    return this.getIcon(iconName) || this.getIcon("info");
  },
};

export default IconRegistry;
