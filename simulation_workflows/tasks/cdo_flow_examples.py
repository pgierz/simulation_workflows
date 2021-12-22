from prefect import Flow
from prefect_cdo import Command

print("\n\n\n")

with Flow(name="example-return-filepath") as flowA:
    cdo = Command()
    response = cdo.run(
        "fldmean",
        input_file="~/Research/reference_stuff/bek_1.nc",
    )
    print("This results in a temporary filepath:")
    print(response)

print(120 * "=")

with Flow(name="example-return-xdataset") as flowB:
    cdo = Command()
    response = cdo.run(
        "fldmean",
        input_file="~/Research/reference_stuff/bek_1.nc",
        returnXDataset=True,
    )
    print(response)
