from typing import Any, Iterable, TypeAlias, Union

from followthemoney.types import registry
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ftmq.types import CE
from ftmq.util import make_proxy

Properties: TypeAlias = dict[str, list[Union[str, "Entity"]]]


class Entity(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., examples=["NK-A7z...."])
    caption: str = Field(..., examples=["John Doe"])
    schema_: str = Field(..., examples=["LegalEntity"], alias="schema")
    properties: Properties = Field(..., examples=[{"name": ["John Doe"]}])
    datasets: list[str] = Field([], examples=[["us_ofac_sdn"]])
    referents: list[str] = Field([], examples=[["ofac-1234"]])

    @classmethod
    def from_proxy(cls, entity: CE, adjacents: Iterable[CE] | None = None) -> "Entity":
        properties = dict(entity.properties)
        if adjacents:
            adjacents = {e.id: Entity.from_proxy(e) for e in adjacents}
            for prop in entity.iterprops():
                if prop.type == registry.entity:
                    properties[prop.name] = [
                        adjacents.get(i, i) for i in entity.get(prop)
                    ]
        return cls(
            id=entity.id,
            caption=entity.caption,
            schema=entity.schema.name,
            properties=properties,
            datasets=list(entity.datasets),
            referents=list(entity.referents),
        )

    def to_proxy(self) -> CE:
        return make_proxy(self.model_dump(by_alias=True))

    @model_validator(mode="before")
    @classmethod
    def get_caption(cls, data: Any) -> Any:
        if data.get("caption") is None:
            proxy = make_proxy(data)
            data["caption"] = proxy.caption
        return data
