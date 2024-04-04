import pathlib

import orjson
from pydantic import BaseModel

from ftmq import model

OUT = pathlib.Path(__file__).parent.absolute()


def write_jsonschema(model: BaseModel):
    with open(OUT / f"{model.__name__}.json", "wb") as f:
        f.write(orjson.dumps(model.model_json_schema()))


if __name__ == "__main__":
    for m in model.__all__:
        m = getattr(model, m)
        write_jsonschema(m)
