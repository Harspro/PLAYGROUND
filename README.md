

Harshit Patel (PCB-C)
0 minutes 6 seconds0:06
Harshit Patel (PCB-C) 0 minutes 6 seconds
Okay, perfect.
Harshit Patel (PCB-C) 0 minutes 9 seconds
Okay, so I was working for the terminal background. Just give me one second here.
Harshit Patel (PCB-C) 0 minutes 20 seconds
Yeah, as per the discussion, and I'll just start, let me start with a problem we are trying to solve. So on the Terminus data platform, we are running around 1000 plus Airflow DAGs across BigQuery, GCS, Dataproc, and Cloud Functions. Right now, there is no automated way to validate data quality,
Harshit Patel (PCB-C) 0 minutes 39 seconds
to detect regression manually, detect regression when code changes. So whenever we touch a shared library, we essentially have to manually regression test hundreds of pipeline. That's very slow, it doesn't scale and it's issues. What?
Harshit Patel (PCB-C) 0 minutes 59 seconds
Basically, we are proposing is a platform native automated data quality and regression testing framework that validates data quality continuously on all critical pipelines, detect regression automatically when shared code. Yes.

Daniel Zhao (PCB)
1 minute 12 seconds1:12
Daniel Zhao (PCB) 1 minute 12 seconds
Sorry, Harshit, this one is the data quality should not be a focus here. Actually, that should be a separate consideration or initiative. This one is more regarding regression testing. Yeah, there it may have some overlap, but the main focus is not really data quality.
HP

Harshit Patel (PCB-C)
1 minute 24 seconds1:24
Harshit Patel (PCB-C) 1 minute 24 seconds
Yeah.
Harshit Patel (PCB-C) 1 minute 32 seconds
Oke.

Daniel Zhao (PCB)
1 minute 33 seconds1:33
Daniel Zhao (PCB) 1 minute 33 seconds
Yeah, this one is more like, you know, we want to for any pipelines we want to run and then cheque whether the success or failure, and also we all focus on requesting testing criteria, and now the data quality itself is a very big topic, so I don't want, you know...
Harshit Patel (PCB-C) 13 minutes 17 seconds
Okay, the good level to check, okay.

Daniel Zhao (PCB)
13 minutes 20 seconds13:20
Daniel Zhao (PCB) 13 minutes 20 seconds
And also, this is not actually true regression, because when you run in the CI/CD pipeline, the environment is different from what our Devin and UET, so even like your test of pass, that does not mean.
HP
Harshit Patel (PCB-C)
13 minutes 37 seconds13:37
Harshit Patel (PCB-C) 13 minutes 37 seconds
Mhm.

Daniel Zhao (PCB)
13 minutes 40 seconds13:40
Daniel Zhao (PCB) 13 minutes 40 seconds
The tag will run successfully in our real environment.
Daniel Zhao (PCB) 13 minutes 44 seconds
The.
HP
Harshit Patel (PCB-C)
13 minutes 46 seconds13:46
Harshit Patel (PCB-C) 13 minutes 46 seconds
Got it, Oke.

Daniel Zhao (PCB)
13 minutes 47 seconds13:47
Daniel Zhao (PCB) 13 minutes 47 seconds
Look, for example, many other data depends on the query. How are you going to simulate the query in CICD pipeline? Does that itself is very difficult?
HP
Harshit Patel (PCB-C)
13 minutes 58 seconds13:58
Harshit Patel (PCB-C) 13 minutes 58 seconds
Oke.
Harshit Patel (PCB-C) 14 minutes
So what would be the best approach if I can ask Daniel?

Daniel Zhao (PCB)
14 minutes14:00
Daniel Zhao (PCB) 14 minutes
Per.
Daniel Zhao (PCB) 14 minutes 5 seconds
So, basically, we will, we will, how we are, we are see our assumption will have one dedicated environment for you to run regression testing. OK, like our, for example, our current UET. Well, for let's assume our current UET will be reserved for a regression testing only.
HP
Harshit Patel (PCB-C)
14 minutes 16 seconds14:16
Harshit Patel (PCB-C) 14 minutes 16 seconds
Oke.
Harshit Patel (PCB-C) 14 minutes 19 seconds
Mhm.

Daniel Zhao (PCB)
14 minutes 26 seconds14:26
Daniel Zhao (PCB) 14 minutes 26 seconds
So all the changes should be applied to be deployed to UET. Doesn't matter if the airflow change or like spark code change or terraform change, they are deployed to our UET. From there, you need to have a process or mechanism to regression testing all our ducks to make sure they do not fail.
HP
Harshit Patel (PCB-C)
14 minutes 33 seconds14:33
Harshit Patel (PCB-C) 14 minutes 33 seconds
Oke.

Daniel Zhao (PCB)
14 minutes 47 seconds14:47
Daniel Zhao (PCB) 14 minutes 47 seconds
Right, that is the main problem we want to solve with the regression problem. That is the part taking the most effort, so we need to have a process or mechanism at a given time, at a given, see, for example, we see the next two weeks we want to do a full regression.
HP
Harshit Patel (PCB-C)
14 minutes 48 seconds14:48
Harshit Patel (PCB-C) 14 minutes 48 seconds
Oke.
MC
Madhu Chatterjee (PCB)
14 minutes 57 seconds14:57
Madhu Chatterjee (PCB) 14 minutes 57 seconds
Yeah.

Daniel Zhao (PCB)
15 minutes 6 seconds15:06
Daniel Zhao (PCB) 15 minutes 6 seconds
Then you need to have a processor mechanism to make sure all our decks will be will be tested, and we have a way to automatically verify where there is pass or fail.
HP
Harshit Patel (PCB-C)
15 minutes 6 seconds15:06
Harshit Patel (PCB-C) 15 minutes 6 seconds
Okay.

Daniel Zhao (PCB)
15 minutes 18 seconds15:18
Daniel Zhao (PCB) 15 minutes 18 seconds
Per.
HP
Harshit Patel (PCB-C)
15 minutes 19 seconds15:19
Harshit Patel (PCB-C) 15 minutes 19 seconds
Mhm.

Daniel Zhao (PCB)
15 minutes 19 seconds15:19
Daniel Zhao (PCB) 15 minutes 19 seconds
Right, that that's the the the problem statement. OK, if we break it down these two details, we have many kind of scenarios, like we have file loading scenario, we have a table loading scenario, we have we can have Kafka, we have a pipeline publish message to Kafka.

Izhaan Zubair (LCL)
15 minutes 20 seconds15:20
Izhaan Zubair (LCL) 15 minutes 20 seconds
Yeah.
Izhaan Zubair (LCL) 15 minutes 23 seconds
****.
HP
Harshit Patel (PCB-C)
15 minutes 24 seconds15:24
Harshit Patel (PCB-C) 15 minutes 24 seconds
Got it.
Harshit Patel (PCB-C) 15 minutes 34 seconds
Ohh.

Daniel Zhao (PCB)
15 minutes 39 seconds15:39
Daniel Zhao (PCB) 15 minutes 39 seconds
Also, you know, we have a schema change, the Terraformer change, all the different changes scenarios, how your approach can cover these scenarios and verify whether it's a success is pass or fail. That's the problem we want to solve. And like the way you presented here, I think it's not not at all.
Daniel Zhao (PCB) 15 minutes 59 seconds
It's not solving the problem we we said here.
HP
Harshit Patel (PCB-C)
16 minutes 2 seconds16:02
Harshit Patel (PCB-C) 16 minutes 2 seconds
Understandable. OK, no, no, I understand now. OK.

Daniel Zhao (PCB)
16 minutes 7 seconds16:07
Daniel Zhao (PCB) 16 minutes 7 seconds
So, and also, for for grid expectation, I think we we we we we used it before it had the biggest problem is it does not come, it's not compatible with the airflow when we install that with the airflow, it's a it's a it has conflict.
HP
Harshit Patel (PCB-C)
16 minutes 7 seconds16:07
Harshit Patel (PCB-C) 16 minutes 7 seconds
Mm.
Harshit Patel (PCB-C) 16 minutes 14 seconds
Per.

Daniel Zhao (PCB)
16 minutes 26 seconds16:26
Daniel Zhao (PCB) 16 minutes 26 seconds
So we cannot actually install great expectation within Airflow. If you want to use it, actually we need to have another Docker or function as the environment to like install it separately. That's actually will also require a lot of work.
HP
Harshit Patel (PCB-C)
16 minutes 43 seconds16:43
Harshit Patel (PCB-C) 16 minutes 43 seconds
Oke.

Izhaan Zubair (LCL)
16 minutes 45 seconds16:45
Izhaan Zubair (LCL) 16 minutes 45 seconds
So Daniel, what's the, so for the regression test, like I know you mentioned all of the different, the flows, but what is the scope of each of those? Like for example, for a file loading, like is it supposed, so when, when, when let's say Arshad develops like the solution, is it supposed to be customizable? Let's say for a file loading that the number of tests I want to do is like one test is the file.
Izhaan Zubair (LCL) 17 minutes 9 seconds
Is it being just there? The second is table populated.

Daniel Zhao (PCB)
17 minutes 11 seconds17:11
Daniel Zhao (PCB) 17 minutes 11 seconds
Yeah, like, well, no, this is a quick simple example for because for regression testing is not not not same as our daily loading, you know, because daily loading we can have like a day by day, the data is different, right? So, for regression, it's not the same for regression, we say the the the input is always the same, the output of the...

Izhaan Zubair (LCL)
17 minutes 20 seconds17:20
Izhaan Zubair (LCL) 17 minutes 20 seconds
Yeah, yeah.
Izhaan Zubair (LCL) 17 minutes 24 seconds
Yeah.

Daniel Zhao (PCB)
17 minutes 30 seconds17:30
Daniel Zhao (PCB) 17 minutes 30 seconds
We should also be always the same if, like, our output changes, then that means our regression failed, right? So, output, like, how do you verify output the same? Basically, we can have a row counter, we can have some, you know, a sum or aggregated value to cheque the output is, you know, or we can even use the, you know, the...

Izhaan Zubair (LCL)
17 minutes 30 seconds17:30
Izhaan Zubair (LCL) 17 minutes 30 seconds
Okay.
Izhaan Zubair (LCL) 17 minutes 37 seconds
Yep.
HP
Harshit Patel (PCB-C)
17 minutes 38 seconds17:38
Harshit Patel (PCB-C) 17 minutes 38 seconds
Yep.

Daniel Zhao (PCB)
17 minutes 51 seconds17:51
Daniel Zhao (PCB) 17 minutes 51 seconds
you know, hash value of the whole file or whole table to make sure that the data does not change, right? That's how we verify, you know, our result is the same, given the same input, our output is the same, right? It's a very, you can, it's a very generic approach, you can, I mean, regarding file loading scenario.
HP
Harshit Patel (PCB-C)
18 minutes 4 seconds18:04
Harshit Patel (PCB-C) 18 minutes 4 seconds
Oke.

Daniel Zhao (PCB)
18 minutes 10 seconds18:10
Daniel Zhao (PCB) 18 minutes 10 seconds
You can have one generic approach, right? For table loading, maybe similarly, but for Kafka, we may have another one, another kind of way to verify whether the message we publish is the message we want. You know, we need to define these approaches, right? And also for, if you have a change for table schema, you know how we verify the.

Izhaan Zubair (LCL)
18 minutes 22 seconds18:22
Izhaan Zubair (LCL) 18 minutes 22 seconds
Good.
Izhaan Zubair (LCL) 18 minutes 24 seconds
Yeah, yeah.
Izhaan Zubair (LCL) 18 minutes 25 seconds
Yep.

Daniel Zhao (PCB)
18 minutes 30 seconds18:30
Daniel Zhao (PCB) 18 minutes 30 seconds
The the the schema before after does not change, or whatever. Maybe we we should have some reports. We already have reports like our data management tool already keeping the schema JSON right, but the way right now we are not keeping for every table like we have many table that's done.

Izhaan Zubair (LCL)
18 minutes 44 seconds18:44
Izhaan Zubair (LCL) 18 minutes 44 seconds
Yes.

Daniel Zhao (PCB)
18 minutes 50 seconds18:50
Daniel Zhao (PCB) 18 minutes 50 seconds
Not in that table, so we cannot regression that part, but maybe we we we should have some way to capture all schema of the all tables when we do regression testing, we can compare. OK, the schema of each table does not change something like that.
Daniel Zhao (PCB) 19 minutes 7 seconds
Yeah, someone to.
HP
Harshit Patel (PCB-C)
19 minutes 7 seconds19:07
Harshit Patel (PCB-C) 19 minutes 7 seconds
Oke.
MC
Madhu Chatterjee (PCB)
19 minutes 9 seconds19:09
Madhu Chatterjee (PCB) 19 minutes 9 seconds
Yeah, so Daniel, what you are saying, according to that, this regression will not run as part of CICD pipeline. There will be a separate environment where we will conduct our regression. But we will have...

Daniel Zhao (PCB)
19 minutes 10 seconds19:10
Daniel Zhao (PCB) 19 minutes 10 seconds
Yeah, so they go to switch out those people by then.
Daniel Zhao (PCB) 19 minutes 18 seconds
This.
Daniel Zhao (PCB) 19 minutes 22 seconds
Yeah.
HP
Harshit Patel (PCB-C)
19 minutes 23 seconds19:23
Harshit Patel (PCB-C) 19 minutes 23 seconds
Oke.

Daniel Zhao (PCB)
19 minutes 24 seconds19:24
Daniel Zhao (PCB) 19 minutes 24 seconds
Yeah, with our assumption, right?
MC
Madhu Chatterjee (PCB)
19 minutes 28 seconds19:28
Madhu Chatterjee (PCB) 19 minutes 28 seconds
Yes, and we will have a mock data.

Daniel Zhao (PCB)
19 minutes 32 seconds19:32
Daniel Zhao (PCB) 19 minutes 32 seconds
Yeah.
MC
Madhu Chatterjee (PCB)
19 minutes 33 seconds19:33
Madhu Chatterjee (PCB) 19 minutes 33 seconds
So, we will have some mock data against which we will test, because it is not possible to test with actual data in regression that will take a lot of time, right?

Daniel Zhao (PCB)
19 minutes 34 seconds19:34
Daniel Zhao (PCB) 19 minutes 34 seconds
Oke.
Daniel Zhao (PCB) 19 minutes 43 seconds
We cannot, right, because of the input, the data keep changing. Like, how can we make sure, you know, other comparing, you know, yesterday run, today run, they are the same, but there's no way, because the each data file is different, right? So, for regression, actually, we should have fixed the input, keep the input that does not change.
HP
Harshit Patel (PCB-C)
26 minutes 1 second26:01
Harshit Patel (PCB-C) 26 minutes 1 second
OK, Daniel. OK, team. Yes, Chris. Yes.

Chris Chalissery (LCL-C)
26 minutes 3 seconds26:03
Chris Chalissery (LCL-C) 26 minutes 3 seconds
A good question. So, so yes, we when we do this regression, so for example, if there is a change for the common template for the daily loading, right? So, for example, there are like 60 daily loading job is happening, right? So, when the changes, we should we test for all the 50?
Chris Chalissery (LCL-C) 26 minutes 21 seconds
or we should our focus will be a sample of DAGs should be tested. How are we?
Chris Chalissery (LCL-C) 26 minutes 31 seconds
Did you get what I meant or did I confuse you also?
MC
Madhu Chatterjee (PCB)
26 minutes 34 seconds26:34
Madhu Chatterjee (PCB) 26 minutes 34 seconds
I think everything should be tested as part of regression.

Chris Chalissery (LCL-C)
26 minutes 37 seconds26:37
Chris Chalissery (LCL-C) 26 minutes 37 seconds
Whichever, yeah, whichever impacted by that library all should be tested, right? That should be our.
MC
Madhu Chatterjee (PCB)
26 minutes 42 seconds26:42
Madhu Chatterjee (PCB) 26 minutes 42 seconds
Yes, yes, or should be. That is like regression. So we have to keep in mind like regression test cases will be small, a subset of.

Daniel Zhao (PCB)
26 minutes 51 seconds26:51
Daniel Zhao (PCB) 26 minutes 51 seconds
Ohh, no, also, I no, I don't want to make it very complicated, right? Like, like, for example, still we go back to the account master file. We may not need to go go back to field by field. We can't just stay general, see what focus on, yeah, we load it to be where we have so many columns, we cut into the whole hash.
Daniel Zhao (PCB) 27 minutes 11 seconds
So, after we have one successful load, we generate a part of each column. We just cheque the hash value does not change. We don't want to cheque a field by field, right? That's what be too complicated.
Daniel Zhao (PCB) 27 minutes 27 seconds
Like, you understand about what I mean, like...
Daniel Zhao (PCB) 27 minutes 30 seconds
For now, maybe let me open a table.
