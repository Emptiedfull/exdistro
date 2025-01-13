document.addEventListener("scroll", function() {
    const hero = document.querySelector(".hero");
    const content = document.querySelector(".content");
    const scrollPosition = window.scrollY;

    if (scrollPosition > 0) {
        hero.classList.add("hidden");
        content.classList.add("content-up");
    } else {
        hero.classList.remove("hidden");
        content.classList.remove("content-up");
    }
});

function scrollByAmount() {
    const scrollAmount = window.innerHeight * 0.5; 
    window.scrollBy({ top: scrollAmount, behavior: 'smooth' });
}