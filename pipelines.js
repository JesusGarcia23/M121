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