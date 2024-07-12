# Lab 1 - MapReduce

## Thoughts

It was interesting to build. Contrary to my first expectations, the worker drives most of the process. The Worker asks for tasks, does the tasks, then reports the completion to the coordinator. The coordinator is only responsible for keeping track of stuff. Which makes sense, I just expected the coordinator to _tell_ the workers what to do, rather than have them request and then it decides.

Failing the early exit test confused me because I had the workers exit when there were no more available tasks. But that raises the problem of what if the worker on the final task hangs or crashes? Then there's no more workers requesting tasks and the coordinator waits forever, unable to finish. So the GetTask function now just sends a "wait" message back until it sees for sure that all tasks are complete.

Having a goroutine launch with a sleep, basically functioning as a timer, was surprisingly intuitive. I got a little chuckle out of how easy it was to reassign tasks with the way I impemented GetTask; just set the status back to "ready" and it's fair game.

Mutex locking bit me at one point. I got overeager and locked/unlocked in a function that was called from a function that was already locking. This made a deadlock and nothing could continue. I wonder if you should spawn that stuff off into a goroutine, or someone _ensure_ that you're in a locked context when that function is called. It was easy enough to just eyeball that it was always called within a locked context, but in a bigger project I could see that getting dangerous.

The Done() function needed locking to clear the last detected race condition. I called this function from within a locked context, though. So I extracted the actual logic of Done() into a private, non-locking done() function, and then called that from Done() and GetTask(), both of which are locked contexts.

## Bonus Challenges

### Implement my own MapReduce plugin

They suggest distributed grep. Could be fun. I already made a copycat of the wc.go plugin as a learning exercise. Distributed grep is a different beast though. Maybe! But I wanna get to the next lectures.

### Set up the workers and coordinators on different machines

The requirement of a shared filesystem is beyond me. They use Athena at MIT which is not accessible and I don't know enough about distributed file systems. Learning everything required to get that running would be a distraction from continuing the course, more than a useful learning exercise.
