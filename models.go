package main

import (
    "gopkg.in/mgo.v2/bson"
)


type Company struct{
    Id bson.ObjectId `bson:"_id"`
    AdminId string `bson:"admin_id"`
    Users []string `bson:"users"`
}