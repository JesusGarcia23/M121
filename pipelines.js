var pipeline = [
    {$match: { 
        $and: [
            {"imdb.rating": {$gte: 7}},
            {genres: {$nin: ["Crime", "Horror"]}},
            {rated: {$in: ["PG","G"]}},
            {languages: {$all: ["English","Japanese"] }}
	]                
    }
},
{
    $project:{ _id: 0, title: 1, rated: 1 }
}]

var pipeline = [
{
$project: {_id: 0, newTitle: { $size: { $split: ["$title", " "] } } }
},
{
$match: {newTitle: 1}
}
]

// Lab: cursor-like Stages
var favorites = [
    "Sandra Bullock",
    "Tom Hanks",
    "Julia Roberts",
    "Kevin Spacey",
    "George Clooney"];

var pipeline = [
    {$match: {
        countries: {$in: ['USA']},
        "tomatoes.viewer.rating": {$gte: 3},
        cast: {$in: favorites}
    }
    },
    {
    $project: {
        _id: 0,
        countries: 1,
        cast: 1,
        title: 1,
        "tomatoes.viewer.rating": 1,
        num_favs: {
            $size: {
                $setIntersection: [
                    "$cast",
                    favorites
                ]
            }
        }
    }
    },
    {
        $sort: {num_fav: -1, "tomatoes.viewer.rating": -1, title: -1 }
    },
    {
        $skip: 24
    },
    {
        $limit: 1
    }
]

// Lab - Bringing it all together

var pipeline = [
    {$match: {
        languages: {$in: ["English"]},
        "imdb.rating": {$gte: 1},
        "imdb.votes": {$gte: 1},
        year: {$gte: 1990}
    }

    },
    {
    $project: {
        title: 1,
        year: 1,
        languages: 1,
        imdb: 1,
        normalized_rating: {$avg: ["$imdb.rating", {$add: [
            1,
            {
                $multiply: [
                9,
                {
                    $divide: [
                    { $subtract: ["$imdb.votes", 5] },
                    { $subtract: [1521105, 5] }
                    ]
                }
                ]
            }
            ]
            }]
        }
    }
    },
    {
        $sort: {"normalized_rating": 1}
    },
    {
        $limit: 1
    }
]

// $group Stage
db.movies.aggregate([
    {
        $group: {
            _id: "$year",
            num_films_in_year: { $sum: 1 } 
        }
    },
    {
        $sort: { num_films_in_year: -1 }
    }
])


// example # 2
db.movies.aggregate([
    {
        $match: {
            metacritic: {$ne: null} 
        }
    },
    {
        $group: {
            _id: {numDirectors: {
                $cond: [{$isArray: "$directors"}, {$size: "$directors"}, 0]
            },
          },
          numFilms: { $sum: 1},
          averageMetacritic: { $avg: "$metacritic" }
        }
    },
    {
        $sort: { "_id.numDirectors": -1 }
    }
])

// example # 3
db.movies.aggregate([
    {
        $match: { metacritic: {$gte: 0} },
    },
    {
        $group: {
            _id: null,
            averageMetacritic: { $avg: "$metacritic"},
        }
    }
])


// Accumulator Stages
//Accumulator expressions:
// - $sum
// - $avg
// - $max
// - $min
// - $stdDevPop (population (whole collection) )
// - $stdDevSam (sample data)
// - $map (for more complex calculations)
// - $reduce (for more complex calculations)




// $reduce (find the maximum value)

db.icecream_data.aggregate([
    {
        $project: {
            _id: 0,
            max_high: {
                $reduce: {
                    input: "$trends",
                    initialValue: -Infinity,
                    in: {
                        $cond: [
                            { $gt: ["$$this.avg_high_tmp", "$$value"] },
                            "$$this.avg_high_tmp",
                            "$$value"
                        ]
                    }
                }
            }
        }
    }
])

// $min (find the minimum value)
db.icecream_data.aggregate([
    {
        $project: {
            _id: 0,
            max_low: {$min: "$trends.avg_low_tmp"}
        }
    }
])

// $avg (find the average)
// $stdDevPop (find the standard deviation)
db.icecream_data.aggregate([
    {
        $project: {
            _id: 0,
            average_cpi: { $avg: "$trends.icecream_cpi" },
            cpi_deviation: { $stdDevPop: "$trends.icecream_cpi"}
        }
    }
])



// $$value = acumulator value
// $$ = acumulator value
// $ = actual value


// Lab - $group and Accumulators
//we group all documents together by specifying null` to ``_id
db.movies.find({awards: {$ne: ""}}, {awards: 1}).pretty()


db.movies.aggregate([
    {
        $match: { 
            awards: {$regex: /Won/ } 
        }
    },
    {
        $group: {
            _id: null,
            highest_rating: { $max: "$imdb.rating" },
            lowest_rating: { $min: "$imdb.rating" },
            average_rating: { $avg: "$imdb.rating" },
            deviation: { $stdDevSamp: "$imdb.rating" }
        }
    }
])



// UNWIND (ONLY WORKS ON ARRAY VALUES)
//GET DOCUMENT WITH ARRAY FIELDS AND MAKE SINGLE DOCUMENTS WITH THE ARRAY VALUES 
db.movies.aggregate([
    {
        $match: {
            "imdb.rating": {$gt: 0 },
            year: { $gte: 2010, $lte: 2015 },
            runtime: { $gte: 90 }
        }
    },
    {
        $unwind: "$genres"
    },
    {
        $group: {
            _id: {
                year: "$year",
                genre: "$genres"
            },
            average_rating: { $avg: "$imdb.rating" }
        }
    },
    {
        $sort: { "_id.year": -1, average_rating: -1 }
    }
])

// get single document with the field wanted

db.movies.aggregate([
    {
        $match: {
            "imdb.rating": {$gt: 0 },
            year: { $gte: 2010, $lte: 2015 },
            runtime: { $gte: 90 }
        }
    },
    {
        $unwind: "$genres"
    },
    {
        $group: {
            _id: {
                year: "$year",
                genre: "$genres"
            },
            average_rating: { $avg: "$imdb.rating" }
        }
    },
    {
        $sort: { "_id.year": -1, average_rating: -1 }
    },
    {
        $group: {
            _id: "$_id.year",
            genre: {$first: "$_id.genre"},
            average_rating: { $first: "$average_rating"}
        }
    },
    {
        $sort: {_id: -1}
    }
])

// Lab - $unwind

db.movies.aggregate([
    {
       $match: {cast: {$exists: true}, languages: {$in: ["English"]} } 
    },
    {
        $project: { _id: 0, cast: 1, "imdb.rating": 1 }
    },
    {
        $unwind: "$cast"
    },
    {
        $group: {
            _id: "$cast",
            numFilms: {$sum: 1},
            average: {$avg: "$imdb.rating"}
        }
    },
    {
        $project: {
            numFilms: 1,
            average: {$trunc: [{$avg: "$average"}, 1]}
        }
    },
    {
        $sort: {numFilms: -1}
    },
    {
        $limit: 1
    }
])


// $LOOKUP (JOIN TWO DOCUMENTS)
// {
//     $lookup: {
//         from: <collection to join>, COLLECTION CANNOT BE SHARDED and MUST BE IN THE SAME DATABASE
//         localField: <field from the input documents>,
//         foreignField: <field from the documents of the "from" collection>,
//         as: <output array field>
//     }
// }

db.air_alliances.aggregate([
    {
        $lookup: {
            from: "air_airlines",
            localField: "airlines",
            foreignField: "name",
            as: "airlines"
        }
    }
]).pretty()


// Lab - Using $lookup
db.air_routes.aggregate([
    {
      $match: {
        airplane: /747|380/
      }
    },
    {
      $lookup: {
        from: "air_alliances",
        foreignField: "airlines",
        localField: "airline.name",
        as: "alliance"
      }
    },
    {
      $unwind: "$alliance"
    },
    {
      $group: {
        _id: "$alliance.name",
        count: { $sum: 1 }
      }
    },
    {
      $sort: { count: -1 }
    }
  ])


// We begin by aggregating over our air_routes collection to allow for filtering of documents containing the string "747" or "380". 
// If we started from air_alliances we would have to do this after the lookup!
// Next, we use the $lookup stage to match documents from air_alliances on the value 
// of their airlines field against the current document's airline.name field
// We then use $unwind on the alliance field we created in $lookup, creating a document with each entry in alliance
// We end with a $group and $sort stage, grouping on the name of the alliance and counting how many times it appeared
// This produces the following output





//GraphLookup: Simple Lookup
// Find all of the descendants documents beneath the specific value (this case name Eliot)
db.parent_reference.aggregate([
    {$match: { name: 'Eliot'}},
    {$graphLookup: {
        from: 'parent_reference',
        startWith: '$_id',
        connectFromField: '_id',
        connectToField: 'reports_to',
        as: 'all_reports'}
    }
])

// Find all the ascendent documents of tree (bottom to top)
db.parent_reference.aggregate([
    {$match: { name: 'Shannon'}},
    {$graphLookup: {
        from: 'parent_reference',
        startWith: '$reports_to',
        connectFromField: 'reports_to',
        connectToField: '_id',
        as: 'bosses'}
    }
])

//graphLookup: Simple Lookup Reverse Schema
db.parent_reference.aggregate([
    {$match: { name: 'Dev'}},
    {$graphLookup: {
        from: 'child_reference',
        startWith: '$direct_reports',
        connectFromField: 'direct_reports',
        connectToField: 'name',
        as: 'all_reports'}
    }
])


// $graphLookup: maxDepth and depthField
// example two levels down
db.child_reference.aggregate([
    {$match: {name: 'Dev'}},
    {$graphLookup: {
        from: 'child_reference',
        startWith: '$direct_reports',
        connectFromField: 'direct_reports',
        connectToField: 'name',
        as: 'till_2_level_reports',
        maxDepth: 1
    }}
])

//find how many recursive lookups needed to find a document
db.child_reference.aggregate([
    {$match: {name: 'Dev'}},
    {$graphLookup: {
        from: 'child_reference',
        startWith: '$direct_reports',
        connectFromField: 'direct_reports',
        connectToField: 'name',
        as: 'descendants',
        maxDepth: 1,
        depthField: 'level' // This does the trick
    }}
])




// $graphLookup: Cross Collection Lookup
db.airlines.aggregate([
    {$match: {name: "TAP Portugal"} },
    { $graphLookup: {
        from: 'routes',
        as: 'chain',
        startWith: '$base',
        connectFromField: 'dst_airport',
        connectToField: 'src_airport',
        maxDepth: 1
    }
}
])

// RESTRIC SEARCH
db.airlines.aggregate([
    {$match: {name: "TAP Portugal"} },
    { $graphLookup: {
        from: 'routes',
        as: 'chain',
        startWith: '$base',
        connectFromField: 'dst_airport',
        connectToField: 'src_airport',
        maxDepth: 1,
        restrictSearchWithMatch: { "airline.name": "TAP Portugal"}
    }
}
])


// LAB: $graphLookup
db.air_alliances.aggregate([
    {
      $match: { name: "OneWorld" }
    },
    {
      $graphLookup: {
        startWith: "$airlines",
        from: "air_airlines",
        connectFromField: "name",
        connectToField: "name",
        as: "airlines",
        maxDepth: 0,
        restrictSearchWithMatch: {
          country: { $in: ["Germany", "Spain", "Canada"] }
        }
      }
    },
    {
      $graphLookup: {
        startWith: "$airlines.base",
        from: "air_routes",
        connectFromField: "dst_airport",
        connectToField: "src_airport",
        as: "connections",
        maxDepth: 1
      }
    },
    {
      $project: {
        validAirlines: "$airlines.name",
        "connections.dst_airport": 1,
        "connections.airline.name": 1
      }
    },
    { $unwind: "$connections" },
    {
      $project: {
        isValid: {
          $in: ["$connections.airline.name", "$validAirlines"]
        },
        "connections.dst_airport": 1
      }
    },
    { $match: { isValid: true } },
    {
      $group: {
        _id: "$connections.dst_airport"
      }
    }
  ])

//This pipeline takes the most selective collection first, air_alliances, matching the document refering to the OneWorld alliance.

//It then iterates, with maxDepth 0 on the air_airlines collection to collect the details on the airlines, specially their base airport, 
//but restricting that $lookup to airlines of the requested countries [Spain, Germany, Canada], using restrictSearchWithMatch.

//We then iterate over all routes up to maximum of one layover by setting our maxDepth to 1. 
//We find all possible destinations when departing from the base airport of each carrier by specify $airlines.base in startWith

//We now have a document with a field named connections that is an array of all routes that are within 1 layover. 
//We use a $project here to remove unnecessary information from the documents.
//We also need to include information about valid airlines that match our initial restriction and the name of the current airline.

//After this, we'll unwind our connections array, and then use $project to add a field representing whether this particular route is valid, meaning 
//it is a route flown by one of our desired carriers.

//Lastly, we use $match to filter out invalid routes, and then $group them on the destination.

//An important aspect to this pipeline is that the first $graphLookup will act as a regular $lookup since we are setting a maxDepth to zero. 
//The reason why we are taking this approach is due to the match restriction that $graphLookup allows, which can make this stage more efficient. 
//Think back to the earlier lab on $lookup, can you think of a way to simplify the aggregation using $graphLookup instead?


// MULTIDIMENSIONAL GROUPING

// SINGLE FACETS
db.companies.aggregate([
    {$match: { '$text': {'$search': 'network'} } },
    {$unwind: '$offices'},
    {$match: {'offices.city': {'$ne': ''} } },
    {$sortByCount: '$offices.city'} // this makes this pipeline a single facet
])

// MANUAL BUCKETS
// Must always specify at least 2 values to boundaries
// boundaries must all be of the same general type (Numeric, String)
// count is inserted by default with no output, but removed when output is specified
db.companies.aggregate([
    {
        $match: {$founded_year: {$gt: 1980} }
    },
    {
        $bucket: {
            groupBy: $number_of_employees,
            boundaries: [0, 20, 50, 100, 500, 1000, Infinity], // List values to group By
            default: 'Other',
            output: {
                total: {$sum: 1},
                average: {$avg: $number_of_employees},
                categories: {$addToSet: $category_code}
            }
        }
    }

])

// AUTO BUCKETS
// Cardinality of groupBy expression may impact even distribution and number of buckets
// specifying a granularity requires the expression to groupBy to resolve to a numeric value
db.companies.aggregate([
    {
        $match: {"$offices.city": 'New York'},
    },
    {
        $bucketAuto: {
            groupBy: $founded_year,
            buckets: 5
        }
    }
])

//MULTIPLE FACETS
db.companies.aggregate([
    {$match: {$text: {$search: 'Databases'} } },
    {
        $facet: {
            'Categories': [{$sortByCount: '$category_code'}],
            'Employees': [
                {$match: {'founded_year': {$gt: 1980} } },
                {$bucket: {
                    groupBy: '$number_of_employees',
                    boundaries: [0, 20, 50, 100, 500, 1000, Infinity],
                    default: 'Other'
                }}
            ],
            'Founded': [
                {$match: {'offices.city': 'New York'} },
                {$bucketAuto: {
                    groupBy: $founded_year,
                    buckets: 5
                }}
            ]
        }
    }
])


// $sortByCount Stage
// is equivalent to a group stage to count occurence, and then sorting ind escending order
{
    $sortByCount: "$<field>"
}

db.movies.aggregate([
    {
      $match: { metacritic: { $gte: 0 }, "imdb.rating": { $gte: 0 } }
    },
    {
      $project: { _id: 0, metacritic: 1, imdb: 1, title: 1 }
    },
    {
      $facet: {
        top_metacritic: [
          {
            $sort: { metacritic: -1, title: 1 }
          },
          {
            $limit: 10
          },
          {
            $project: { title: 1 }
          }
        ],
        top_imdb: [
          {
            $sort: { "imdb.rating": -1, title: 1 }
          },
          {
            $limit: 10
          },
          {
            $project: { title: 1 }
          }
        ]
      }
    },
    {
      $project: {
        movies_in_both: {
          $setIntersection: ["$top_metacritic", "$top_imdb"]
        }
      }
    }
  ]);



//   $REDACT STAGE
// PROTECT INFORMATION FROM UNAUNTHORIZED ACCESS
{
    $cond: [{$in: ["Management", "$acl"]}, "$$DESCEND", "$$PRUNE" ]
}
// Mangament would be de value looking for
// $acl was the field chose

// $$KEEP and $$PRUNE automatically apply to all levels below the evaluated level
// $$DESCEND retains the current level and evaluates the next level down
// $redact is not restricting access to a collection



// $OUT STAGE
// USED FOR PERSISTING THE RESULT OF AN AGGREGATION

db.collection.aggregate([
    {$stage1}, {$stage2}, ..., {$stageN}, {$out: "new_collection"}
])

// Will create a new collection or overwrite an existing collection if specified
// Honors indexes on existing collection
// Will not create or overwrite data if pipeline errors.
// Creates collections in the same database as the source collection.
// collections created by $out can't be sharded


// $MERGE STAGE
// Allows flexible ways of saving results of the aggregation into an already existing collection whether or not it's sharded or unsharded
// or different database (better than $out)

{
    $merge: {
        into: "target",
        on: "field"
    }
}

//simple syntax when using same db (into)
{$merge: {"collection2"}}

//simple syntax when using another db (into)
{$merge: { db: "db2", coll:"collection2"}}

//on: "_id" // unsharded
//on: ["_id", "shardkey(s)"] // if the collection is sharded
//on ["field1", "field2"] // if there is multiple fields you want to filter

// must be unique index if you provide your own merging key


{
    $merge: {
      into: "target",
      whenNotMatched: "insert"|"discard"|"fail",
      whenMatched: "merge"|"replace"|"keepExisting"|"fail"
    }
}

//$merge updates fields from mflix.users collection into sv.users collection. Our "_id" field is unique username

// (in mflix) db.users.aggregate(mflix_pipeline)

mflix_pipeline = [
    {"project": {
        "_id": "$username",
        "mflix": "$$ROOT"
    }},
    {"$merge": {
        "into": {
            "db": "sv",
            "collection": "users"
        },
        "whenNotMatched": "discard"
    }

    }
]


//$merge updates fields from mfriendbook.users collection into sv.users collection. Our "_id" field is unique username

// (in mfriendbook) db.users.aggregate(mfriendbook_pipeline)

mfriendbook_pipeline = [
    {"project": {
        "_id": "$username",
        "mfriendbook": "$$ROOT"
    }},
    {"$merge": {
        "into": {
            "db": "sv",
            "collection": "users"
        },
        "whenNotMatched": "discard"
    }

    }
]


// USING $MERGE FOR A TEMPORARY COLLECTION

// aggregate 'temp' and append valid records to 'data'

db.temp.aggregate([
    {...}, // pipeline to massage and cleanse data in temp
    {$merge: {
        into: "data",
        whenMatched: "fail"
    }}
])




// VIEWS
// db.createView(<view>, <source>, <pipeline>, <collation>)
// source is the source collection
//View definitions are public
// Avoid referring to sensitive fields within the pipeline that defines a view
//Views contain no data themselves. They are created on demand and reflect the data in the source collection
// Views are read only. Write operations to views will error
// Views have som restrictions. They must abide by the rules of the Aggregation Framework, and cannot contain find() projection operators
// Horizontal slicing is performed with the $match stage, reducing the number of documents that are returned.
// Vertical slicing is performed with a $project or other shaping stage, modifying individual documents.

db.createView("bronze_banking", "customers", [
    {
        $match: {accountType: "bronze"}
    },
    {
        $project: {
            _id: 0,
            name: {
                $concat: [
                    { $cond: [{$eq: ["$gender", "female"] }, "Miss", "Mr."] },
                    " ", "$name.first", " ","$name.last"
                ]
            },
            phone: 1,
            email: 1,
            address: 1,
            account_ending: { $substr: ["$accountNumber", 7, -1] }
        }
    }
])



// AGGREGATION PERFORMANCE

// Realtime Processing

// - provide data for applications
// - Query performance is more important

// Batch Processing

// - Provide data for analytics
// - Query performance is less important

// Index Usage
db.orders.createIndex({cust_id: 1})
// top-k sorting algorithm
db.orders.aggregate([
    {$match: {cust_id: {$lt: 50} }},
    {$limit: 10},
    {$sort: {total: 1}}
])

// Memory Constraints
// Results are subject to 16MB document limit
    // Use $limit and $project
// 100MB of RAM per stage
    // Use indexes
    db.orders.aggregate([...], {allowDiskUse: true})
        // Doesn't work with $graphLookup (this does not support splitting to disk)



// Aggregation Pipeline on a Sharded Cluster

// Aggregation Optimizations
db.restaurants.aggregate([
    {
        $match: {cuisine: 'Sushi'}
    },
    {
        $sort: {stars: -1}
    }
])

db.restaurants.aggregate([
    {
        $limit: 15
    },
    {
        $skip: 10
    }
])

// Aggregation Pipeline on a Sharded Cluster
sh.shardCollection('m201.restaurant', {"address.state": 1})
db.restaurants.aggregate([
    {
        $match: {'address.state': 'NY'}
    },
    {
        $group: {
            _id: '$address.state',
            avgStars: {$avg: '$stars'}
        }
    }
])

// PIPELINE OPTIMIZATION #1

db.movies.aggregate([
    {
        $match: {
            title: /^[aeiou]/i
        }
    },
    {
        $project: {
            title_size: {$size: { $split: ["$title", " "] } },
        }
    },
    {
        $group: {
            _id: "$title_size",
            count: { $sum: 1 },
        }
    },
    {
        $sort: { count: -1}
    }
])

//OPTIMIZED VERSION

db.movies.aggregate([
    {
        $match: {
            title: /^[aeiou]/i
        }
    },
    {
        $group: {
            _id: {
                $size: { $split: ["$title", " "] }
            },
            count: { $sum: 1 },
        }
    },
    {
        $sort: { count: -1}
    }
])


// Use accumulator expressions, $map, $reduce, and $filter in project before an $unwind, if possible
// Every high order array function can be implemented with $reduce if the provided expressions do not meet your needs.












// EXERCISE

  //Using the air_alliances and air_routes collections, find which alliance has the most unique carriers(airlines) operating between the airports JFK and LHR, 
  // in either directions.

  //Names are distinct, i.e. Delta != Delta Air Lines
  
  // src_airport and dst_airport contain the originating and terminating airport information.  

db.air_routes.aggregate([
    {
      $match: {
        src_airport: { $in: ["LHR", "JFK"] },
        dst_airport: { $in: ["LHR", "JFK"] }
      }
    },
    {
      $lookup: {
        from: "air_alliances",
        foreignField: "airlines",
        localField: "airline.name",
        as: "alliance"
      }
    },
    {
      $match: { alliance: { $ne: [] } }
    },
    {
      $addFields: {
        alliance: { $arrayElemAt: ["$alliance.name", 0] }
      }
    },
    {
      $group: {
        _id: "$airline.id",
        alliance: { $first: "$alliance" }
      }
    },
    {
      $sortByCount: "$alliance"
    }
  ])

  // We begin with a $match stage and fetch routes that originate or end at either LHR and JFK

  // We then $lookup into the air_alliances collection, matching member airline names in the airlines field to the local airline.name field in the route

  // We follow with a $match stage to remove routes that are not members of an alliance. 
  // We use $addFields to cast just the name of the alliance and extract a single element in one go

  // Lastly, we $group on the airline.id, since we don't want to count the same airline twice. We take the $first alliance name to avoid duplicates. 
  // Then, we use $sortByCount to get our answer from the results




























//We begin with a $match and $project stage to only look at documents with the relevant fields, and project away needless information

// Next follows our $facet stage.
// Within each facet, we need sort in descending order for metacritic and imdb.ratting and ascending for title, limit to 10 documents, then only retain the title

// Lastly, we use a $project stage to find the intersection of top_metacritic and top_imdb, producing the titles of movies in both categories




// $project:{_id: 0, newTitle: { $size: { $split: ["$title", " "] } } }
// $match: {newTitle: 1}


// ANOTHER PIPELINE

//  {
//     $match: {
//       cast: { $elemMatch: { $exists: true } },
//       directors: { $elemMatch: { $exists: true } },
//       writers: { $elemMatch: { $exists: true } }
//     }
//   },
//   {
//     $project: {
//       _id: 0,
//       cast: 1,
//       directors: 1,
//       writers: {
//         $map: {
//           input: "$writers",
//           as: "writer",
//           in: {
//             $arrayElemAt: [{ $split: ["$$writer", " ("] }, 0]
//           }
//         }
//       }
//     }
//   },
//   {
//     $project: {
//       labor_of_love: {
//         $gt: [ { $size: { $setIntersection: ["$cast", "$directors", "$writers"] } }, 0 ]
//       }
//     }
//   },
//   {
//     $match: { labor_of_love: true }
//   },
//   {
//     $count: "labors of love"
//   }