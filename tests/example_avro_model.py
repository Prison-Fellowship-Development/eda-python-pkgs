from dataclasses_avroschema.pydantic import AvroBaseModel


class ExampleAvroModel(AvroBaseModel):
    """
    Example Avro model for testing purposes.
    """

    name: str
    age: int
    is_active: bool

    class Config:
        title = "AvroModel"
        description = "An example Avro model for testing purposes."
        json_schema_extra = {
            "example": {"name": "John Doe", "age": 30, "is_active": True}
        }
