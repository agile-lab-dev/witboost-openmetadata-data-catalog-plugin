from pydantic import BaseModel


class CustomUrlPickerError(BaseModel):
    error: str
    suggestion: str | None = None


class CustomUrlPickerItem(BaseModel):
    id: str
    glossary: str
    name: str
    fqn: str


class CustomUrlPickerMalformedRequestError(BaseModel):
    errors: list[str]


class CustomUrlPickerResourcesRequestBody(BaseModel):
    domain: str | None = None


class CustomUrlPickerSystemError(BaseModel):
    errors: list[str]


class CustomUrlPickerValidationError(BaseModel):
    errors: list[CustomUrlPickerError]


class CustomUrlPickerValidationRequest(BaseModel):
    selectedObjects: list[CustomUrlPickerItem]
    queryParameters: CustomUrlPickerResourcesRequestBody | None = None
