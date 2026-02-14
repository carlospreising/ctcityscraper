from schema.property_objects import Property, Base

property = Property(url="https://gis.vgsi.com/NewHavenCT",pid=82)

property.load_buildings()
property.load_ownership()
property.load_appraisal()
property.load_assesment()

print("Property Data: " + str(property.data))

# print(property.buildings[0].data)
# print(property.ownership[0].data)
# print(property.assesments[0].data)
# print(property.appraisals[0].data)

# print("Address: " + property.address)
# print("First Building Info: " + property.buildings[0].num_kitchens)

# print(property.get_dict())
